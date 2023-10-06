package alluxio.worker.ucx;

import alluxio.AlluxioURI;
import alluxio.client.file.cache.LocalCacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.proto.dataserver.Protocol;
import alluxio.table.ProtoUtils;
import alluxio.util.ThreadFactoryUtils;

import alluxio.util.io.BufferPool;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.netty.ReadRequest;

import com.google.protobuf.InvalidProtocolBufferException;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
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
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UcpServer {
  public static final int PAGE_SIZE = 4096;

  private static final Logger LOG = LoggerFactory.getLogger(UcpServer.class);
  private LocalCacheManager mlocalCacheManager;
  private UcpWorker mGlobalWorker;
  private Map<PeerInfo, UcpEndpoint> mPeerEndpoints = new ConcurrentHashMap<>();
  // TODO(lucy) backlogging if too many incoming req...
  private LinkedBlockingQueue<UcpConnectionRequest> mConnectionRequests
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

  public UcpServer() {
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
    int contentLen = buf.getInt();
    buf.limit(buf.position() + contentLen);
    Protocol.ReadRequest request = Protocol.ReadRequest.parseFrom(buf);
    return request;
  }

  class RPCMessageHandler implements Runnable {
    private static final long WORKER_PAGE_SIZE = 1*1024*1024L;
    Protocol.ReadRequest readRequest = null;
    UcpEndpoint remoteEp;
        //conf.getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);

    @Override
    public void run() {
      final String fileId =
          new AlluxioURI(readRequest.getOpenUfsBlockOptions().getUfsPath()).hash();
      int pageIndex = (int)(readRequest.getOffset() / WORKER_PAGE_SIZE);
      int pageOffset = (int)(readRequest.getOffset() % WORKER_PAGE_SIZE);
      long totalLength = readRequest.getLength();
      PageId pageId = new PageId(fileId, pageIndex);
      for (int bytesRead = 0; bytesRead < totalLength; ) {
        int readLen = (int)Math.min(totalLength - bytesRead, WORKER_PAGE_SIZE);
        try {
          Optional<UcpMemory> readContentUcpMem =
              mlocalCacheManager.get(pageId, pageOffset, readLen);
          if (!readContentUcpMem.isPresent()) {
            break;
          }
          UcpRequest sendReq = remoteEp.sendStreamNonBlocking(
              readContentUcpMem.get().getAddress(),
              readContentUcpMem.get().getLength(), new UcxCallback() {
                public void onSuccess(UcpRequest request) {
                  LOG.info("send complete for pageid:{}:pageoffset:{}:readLen:{}",
                      pageId, pageOffset, readLen);
                }

                public void onError(int ucsStatus, String errorMsg) {
                  LOG.error("error sending:pageid:{}:pageoffset:{}:readLen:{}",
                      pageId, pageOffset, readLen);
                }
              });
        } catch (PageNotFoundException | IOException e) {
          throw new RuntimeException(e);
        }
        LOG.info("Handle read req:{} complete", readRequest);
      }
    }


    public RPCMessageHandler(PeerInfo peerInfo, ByteBuffer recvBuffer) {
      // deserialize rpc mesg
      readRequest = null;
      remoteEp = mPeerEndpoints.get(peerInfo);
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

    public UcpRequest recvRequest() {
      ByteBuffer recvBuffer = BufferPool.getInstance().getABuffer(PAGE_SIZE, true)
          .nioBuffer();
      UcpRequest recvRequest = mGlobalWorker.recvTaggedNonBlocking(
          recvBuffer, new UcxCallback() {
            public void onSuccess(UcpRequest request) {
              LOG.info("New req received!");
              PeerInfo peerInfo;
              try {
                peerInfo = PeerInfo.parsePeerInfo(recvBuffer);
              } catch (IOException e) {
                LOG.error("parse peerinfo errored out:", e);
                throw new RuntimeException(e);
              }
              tpe.execute(new RPCMessageHandler(peerInfo, recvBuffer));
            }

            public void onError(int ucsStatus, String errorMsg) {
              LOG.error("Receive req errored, status:{}, errMsg:{}",
                  ucsStatus, errorMsg);
            }
          });
      return recvRequest;
    }

    public void acceptNewConn() {
      UcpConnectionRequest connectionReq = mConnectionRequests.poll();
      if (connectionReq != null) {
        PeerInfo peerInfo = new PeerInfo(
            connectionReq.getClientAddress(),  connectionReq.getClientId());
        UcpEndpoint existingEndpoint = mPeerEndpoints.computeIfAbsent(peerInfo, pInfo -> {
          return mGlobalWorker.newEndpoint(new UcpEndpointParams()
              .setPeerErrorHandlingMode()
              .setConnectionRequest(connectionReq));
        });
        if (existingEndpoint != null) {
          LOG.warn("Existing endpoint found from same peer:" + peerInfo.toString());
        }
      }
    }

    @Override
    public void run() {
      try {
        UcpRequest recvReq = recvRequest();
        while (true) {
          acceptNewConn();
          if (recvReq.isCompleted()) {
            // Process 1 recv request at a time.
            recvReq = recvRequest();
          }
          try {
            if (mGlobalWorker.progress() == 0) {
              mGlobalWorker.waitForEvents();
            }
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