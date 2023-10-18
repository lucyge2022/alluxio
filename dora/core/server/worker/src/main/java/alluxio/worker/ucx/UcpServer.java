package alluxio.worker.ucx;

import alluxio.AlluxioURI;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.LocalCacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.conf.Configuration;
import alluxio.exception.PageNotFoundException;
import alluxio.proto.dataserver.Protocol;
import alluxio.ucx.UcpUtils;
import alluxio.util.ThreadFactoryUtils;

import alluxio.util.io.BufferPool;

import com.google.protobuf.InvalidProtocolBufferException;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpConnectionRequest;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerConnectionHandler;
import org.openucx.jucx.ucp.UcpListenerParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UcpServer {
  public static final int PAGE_SIZE = 4096;

  private static final Logger LOG = LoggerFactory.getLogger(UcpServer.class);
  private LocalCacheManager mlocalCacheManager;
  private UcpWorker mGlobalWorker;
  private Map<PeerInfo, UcpEndpoint> mPeerEndpoints = new ConcurrentHashMap<>();
  private Map<PeerInfo, AtomicLong> mPeerToSequencers = new ConcurrentHashMap<>();
  // TODO(lucy) backlogging if too many incoming req...
  private LinkedBlockingQueue<UcpConnectionRequest> mConnectionRequests
      = new LinkedBlockingQueue<>();
  private LinkedBlockingQueue<UcpRequest> mReceiveRequests
      = new LinkedBlockingQueue<>();
  private static final UcpContext sGlobalContext = new UcpContext(new UcpParams()
        .requestStreamFeature()
        .requestTagFeature()
        .requestWakeupFeature());
  private int BIND_PORT = 1234;

  private AcceptorThread mAcceptorLoopThread;

  private ThreadPoolExecutor tpe = new ThreadPoolExecutor(4, 4,
      0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(4),
      ThreadFactoryUtils.build("worker-threads-%d", true));

  private ExecutorService mAcceptorExecutor =  Executors.newFixedThreadPool(1);

  public UcpServer() throws IOException {
    CacheManagerOptions cacheManagerOptions =
        CacheManagerOptions.createForWorker(Configuration.global());
    mlocalCacheManager = LocalCacheManager.create(
        cacheManagerOptions, PageMetaStore.create(
            CacheManagerOptions.createForWorker(Configuration.global())));
    mGlobalWorker = sGlobalContext.newWorker(new UcpWorkerParams());
    List<InetAddress> addressesToBind = getAllAddresses();
    UcpListenerParams listenerParams = new UcpListenerParams()
        .setConnectionHandler(new UcpListenerConnectionHandler() {
          @Override
          public void onConnectionRequest(UcpConnectionRequest connectionRequest) {
            LOG.info("Incoming request, clientAddr:{} clientId:{}",
                connectionRequest.getClientAddress(), connectionRequest.getClientId());
            mConnectionRequests.offer(connectionRequest);
          }
        });
    for (InetAddress addr : addressesToBind) {
      UcpListener ucpListener = mGlobalWorker.newListener(listenerParams.setSockAddr(
          new InetSocketAddress(addr, BIND_PORT)));
      LOG.info("Bound UcpListener on address:{}", ucpListener.getAddress());
    }
    mAcceptorExecutor.submit(new AcceptorThread());
  }

  public void awaitTermination() {
    mAcceptorExecutor.shutdown();
  }

  public static void main(String[] args) {
    try {
      LOG.info("Starting ucp server...");
      UcpServer ucpServer = new UcpServer();
      LOG.info("Awaiting termination...");
      ucpServer.awaitTermination();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  public static class PeerInfo {
    InetSocketAddress mRemoteAddr;
    long mClientId;
    public PeerInfo(InetSocketAddress remoteAddr, long clientId) {
      mRemoteAddr = remoteAddr;
      mClientId = clientId;
    }

    public static byte[] serialize(PeerInfo peerInfo) throws IOException {
      try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
        try (ObjectOutputStream o = new ObjectOutputStream(b)) {
          o.writeObject(peerInfo);
        }
        return b.toByteArray();
      }
    }

    public static PeerInfo parsePeerInfo(ByteBuffer recvBuffer) throws IOException {
      // Need a common util of serialization
      recvBuffer.clear();
      int peerInfoLen = recvBuffer.getInt();
      byte[] peerInfoBytes = new byte[peerInfoLen];
      recvBuffer.get(peerInfoBytes);
      try (ByteArrayInputStream b = new ByteArrayInputStream(peerInfoBytes)) {
        try (ObjectInputStream o = new ObjectInputStream(b)) {
          return (PeerInfo) o.readObject();
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    }

    public String toString() {
      return "PeerInfo:addr:" + mRemoteAddr.toString()
           + ":clientid:" + mClientId;
    }
  }

  public static List<InetAddress> getAllAddresses() {
    // Get all NIC addrs
    Stream<NetworkInterface> nics = Stream.empty();
    try {
      nics = Collections.list(NetworkInterface.getNetworkInterfaces()).stream()
          .filter(iface -> {
            try {
              return iface.isUp() && !iface.isLoopback() &&
                  !iface.isVirtual() &&
                  !iface.getName().contains("docker");
              // identify infiniband usually interface name looks like ib-...
            } catch (SocketException e) {
              return false;
            }
          });
    } catch (SocketException e) {
    }
    List<InetAddress> addresses = nics.flatMap(iface ->
            Collections.list(iface.getInetAddresses()).stream())
        .filter(addr -> !addr.isLinkLocalAddress())
        .collect(Collectors.toList());
    return addresses;
  }


  public static Protocol.ReadRequest parseReadRequest(ByteBuffer buf)
      throws InvalidProtocolBufferException {
    LOG.info("buf:capacity:{}:position:{}:limit:{}",
        buf.capacity(), buf.position(), buf.limit());
    int contentLen = buf.getInt();
    buf.limit(buf.position() + contentLen);
    Protocol.ReadRequest request = Protocol.ReadRequest.parseFrom(buf);
    return request;
  }

  class RPCMessageHandler implements Runnable {
    private static final long WORKER_PAGE_SIZE = 1*1024*1024L;
    Protocol.ReadRequest readRequest = null;
    UcpEndpoint remoteEp;
    AtomicLong sequencer;
        //conf.getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);

    @Override
    public void run() {
      final String fileId =
          new AlluxioURI(readRequest.getOpenUfsBlockOptions().getUfsPath()).hash();
      long offset = readRequest.getOffset();
      long totalLength = readRequest.getLength();
      List<UcpRequest> requests = new ArrayList<>();
      for (int bytesRead = 0; bytesRead < totalLength; ) {
        int pageIndex = (int)(offset / WORKER_PAGE_SIZE);
        int pageOffset = (int)(offset % WORKER_PAGE_SIZE);
        int readLen = (int)Math.min(totalLength - bytesRead, WORKER_PAGE_SIZE - pageOffset);
        PageId pageId = new PageId(fileId, pageIndex);
        try {
          Optional<UcpMemory> readContentUcpMem =
              mlocalCacheManager.get(pageId, pageOffset, readLen);
          if (!readContentUcpMem.isPresent()) {
            break;
          }
          offset += readLen;
          bytesRead += readLen;
          // first 8 bytes -> sequence  second 8 bytes -> size
          ByteBuffer preamble = ByteBuffer.allocateDirect(16);
          preamble.clear();
          long seq = sequencer.incrementAndGet();
          preamble.putLong(seq);
          preamble.putLong(readContentUcpMem.get().getLength());
          preamble.clear();
          UcpRequest preambleReq = remoteEp.sendStreamNonBlocking(UcxUtils.getAddress(preamble),
              16, new UcxCallback() {
                public void onSuccess(UcpRequest request) {
                  LOG.info("preamble sent, sequence:{}, len:{}"
                      ,seq, readContentUcpMem.get().getLength());
                }

                public void onError(int ucsStatus, String errorMsg) {
                  LOG.error("error sending preamble:pageoffset:{}:readLen:{}",
                      pageOffset, readLen);
                }
              });
          requests.add(preambleReq);

          long[] addrs = new long[2];
          long[] sizes = new long[2];
          ByteBuffer seqBuf = ByteBuffer.allocateDirect(8);
          seqBuf.putLong(seq); seqBuf.clear();
          addrs[0] = UcxUtils.getAddress(seqBuf); sizes[0] = 8;
          addrs[1] = readContentUcpMem.get().getAddress(); sizes[1] = readContentUcpMem.get().getLength();
          UcpRequest sendReq = remoteEp.sendStreamNonBlocking(
              addrs, sizes, new UcxCallback() {
                public void onSuccess(UcpRequest request) {
                  LOG.info("send complete for pageoffset:{}:readLen:{}",
                      pageOffset, readLen);
                  readContentUcpMem.get().deregister();
                }

                public void onError(int ucsStatus, String errorMsg) {
                  LOG.error("error sending :pageoffset:{}:readLen:{}",
                      pageOffset, readLen);
                  readContentUcpMem.get().deregister();
                }
              });
          requests.add(sendReq);
        } catch (PageNotFoundException | IOException e) {
          throw new RuntimeException(e);
        }
      }// end for
      LOG.info("Handle read req:{} complete", readRequest);
      while (requests.stream().anyMatch(r -> !r.isCompleted())) {
        LOG.info("Wait for all {} ucpreq to complete, sleep for 5 sec...");
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    public RPCMessageHandler(PeerInfo peerInfo, ByteBuffer recvBuffer) {
      // deserialize rpc mesg
      readRequest = null;
      remoteEp = mPeerEndpoints.get(peerInfo);
      sequencer = mPeerToSequencers.computeIfAbsent(peerInfo, pi -> new AtomicLong(0L));
      if (remoteEp == null) {
        throw new RuntimeException("unrecognized peerinfo:" + peerInfo.toString());
      }
      try {
        readRequest = parseReadRequest(recvBuffer);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }

  // accept one single rpc req at a time
  class AcceptorThread implements Runnable {

    public UcpRequest recvRequest(PeerInfo peerInfo) {
      if (peerInfo == null) {
        return null;
      }
//      ByteBuffer recvBuffer = BufferPool.getInstance().getABuffer(PAGE_SIZE, true)
//          .nioBuffer();
      ByteBuffer recvBuffer = ByteBuffer.allocateDirect(PAGE_SIZE);
      LOG.info("recvBuffer capacity:{}:limit:{}:position:{}",
          recvBuffer.capacity(), recvBuffer.limit(), recvBuffer.position());
      recvBuffer.clear();
      long tag = UcpUtils.generateTag(peerInfo.mRemoteAddr);
      // currently only matching the ip, excluding port as ucx 1.15.0 doesn't expose ucpendpoint.getloaladdress
      // when client establish remote conn
      long tagMask = 0xFFFFFFFF0000L;
      UcpRequest recvRequest = mGlobalWorker.recvTaggedNonBlocking(
          recvBuffer, tag, tagMask, new UcxCallback() {
            public void onSuccess(UcpRequest request) {
              LOG.info("New req received from peer:{}", peerInfo);
              tpe.execute(new RPCMessageHandler(peerInfo, recvBuffer));
              LOG.info("onSuccess start receiving another req for peer:{}", peerInfo);
              recvRequest(peerInfo);
            }

            public void onError(int ucsStatus, String errorMsg) {
              LOG.error("Receive req errored, status:{}, errMsg:{}",
                  ucsStatus, errorMsg);
              LOG.info("onError start receiving another req for peer:{}", peerInfo);
              recvRequest(peerInfo);
            }
          });
      return recvRequest;
    }

    public void acceptNewConn() {
      UcpConnectionRequest connectionReq = mConnectionRequests.poll();
      if (connectionReq != null) {
        PeerInfo peerInfo = new PeerInfo(
            connectionReq.getClientAddress(), connectionReq.getClientId());
        final AtomicReference<Boolean> newConn = new AtomicReference<>(false);
        mPeerEndpoints.compute(peerInfo, (pInfo, ep) -> {
          if (ep == null) {
            newConn.compareAndSet(false, true);
            return mGlobalWorker.newEndpoint(new UcpEndpointParams()
                .setPeerErrorHandlingMode()
                .setConnectionRequest(connectionReq));
          } else {
            LOG.info("Endpoint for peer:{} already exist, rejecting connection req...", peerInfo);
            connectionReq.reject();
            return ep;
          }
        });
        if (newConn.get()) {
          UcpRequest req = recvRequest(peerInfo);
//          mReceiveRequests.offer(req);
        }
      }
    }

    @Override
    public void run() {
      while (true) {
        try {
          acceptNewConn();
          while (mGlobalWorker.progress() == 0) {
            LOG.info("nothing to progress. wait for events..");
            try {
              mGlobalWorker.waitForEvents();
            } catch (Exception e) {
              LOG.error(e.getLocalizedMessage());
            }
          }
        } catch (Exception e) {
          // not sure what exception would be thrown here.
          LOG.info("Exception in AcceptorThread", e);
        }
      }
    }
  }
}