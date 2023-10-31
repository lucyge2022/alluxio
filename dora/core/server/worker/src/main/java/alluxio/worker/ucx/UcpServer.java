package alluxio.worker.ucx;

import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.LocalCacheManager;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.conf.Configuration;
import alluxio.proto.dataserver.Protocol;
import alluxio.ucx.UcpUtils;
import alluxio.util.ThreadFactoryUtils;

import com.google.protobuf.InvalidProtocolBufferException;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpConnectionRequest;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerConnectionHandler;
import org.openucx.jucx.ucp.UcpListenerParams;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UcpServer {
  private static final Logger LOG = LoggerFactory.getLogger(UcpServer.class);

  public static UcpServer sInstance = null;
  public static ReentrantLock sInstanceLock = new ReentrantLock();
  public static final UcpContext sGlobalContext = new UcpContext(new UcpParams()
      .requestStreamFeature()
      .requestTagFeature()
      .requestRmaFeature()
      .requestWakeupFeature());
  private int BIND_PORT = 1234;

  private UcpWorker mGlobalWorker;
  private Map<PeerInfo, UcpEndpoint> mPeerEndpoints = new ConcurrentHashMap<>();
  private Map<PeerInfo, AtomicLong> mPeerToSequencers = new ConcurrentHashMap<>();
  public LocalCacheManager mlocalCacheManager;
  // TODO(lucy) backlogging if too many incoming req...
  private LinkedBlockingQueue<UcpConnectionRequest> mConnectionRequests
      = new LinkedBlockingQueue<>();
  private LinkedBlockingQueue<UcpRequest> mReceiveRequests
      = new LinkedBlockingQueue<>();
  private AcceptorThread mAcceptorLoopThread;

  private ThreadPoolExecutor tpe = new ThreadPoolExecutor(4, 4,
      0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(4),
      ThreadFactoryUtils.build("worker-threads-%d", true));

  private ExecutorService mAcceptorExecutor =  Executors.newFixedThreadPool(1);

  public static UcpServer getInstance() {
    if (sInstance != null)
      return sInstance;
    sInstanceLock.lock();
    try {
      if (sInstance != null)
        return sInstance;
      try {
        sInstance = new UcpServer();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return sInstance;
    } finally {
      sInstanceLock.unlock();
    }
  }

  public UcpServer() throws IOException {
    CacheManagerOptions cacheManagerOptions =
        CacheManagerOptions.createForWorker(Configuration.global());
    mlocalCacheManager = LocalCacheManager.create(
        cacheManagerOptions, PageMetaStore.create(
            CacheManagerOptions.createForWorker(Configuration.global())));
    mGlobalWorker = sGlobalContext.newWorker(new UcpWorkerParams()
            .requestWakeupRMA());
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

  public UcpWorker getGlobalWorker() {
    return mGlobalWorker;
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

  // accept one single rpc req at a time
  class AcceptorThread implements Runnable {

    public UcpRequest recvEstablishConnRequest() {
      ByteBuffer establishConnBuf = ByteBuffer.allocateDirect(AlluxioUcxUtils.METADATA_SIZE_COMMON);
      // no need to register this buffer to UcpMemory,
      // will go out of scope once call back is done handling
      UcpRequest establishConnReq = mGlobalWorker.recvTaggedNonBlocking(establishConnBuf,
          new UcxConnection.UcxConnectionEstablishCallBack(establishConnBuf, mGlobalWorker));
      return establishConnReq;
    }

    public void acceptNewConn() {
      UcpConnectionRequest connectionReq = mConnectionRequests.poll();
      if (connectionReq != null) {
        PeerInfo peerInfo = new PeerInfo(
            connectionReq.getClientAddress(), connectionReq.getClientId());
        final AtomicReference<Boolean> newConn = new AtomicReference<>(false);
        UcpEndpoint clientEpForConnect = mGlobalWorker.newEndpoint(new UcpEndpointParams()
            .setPeerErrorHandlingMode()
            .setConnectionRequest(connectionReq));
        clientEpForConnect.close();

        mPeerEndpoints.compute(peerInfo, (pInfo, ep) -> {
          if (ep == null) {
            newConn.compareAndSet(false, true);
            return mGlobalWorker.newEndpoint(new UcpEndpointParams()
                .setErrorHandler((errHandleEp, status, errorMsg) -> {
                  UcpEndpoint connectedEp = mPeerEndpoints.remove(peerInfo);
                  if (connectedEp != null) {
                    LOG.info("Closing peer:{} on error:{}", peerInfo, errorMsg);
                    connectedEp.close();
                  }
                    })
                .setPeerErrorHandlingMode()
                .setConnectionRequest(connectionReq));
          } else {
            LOG.info("Endpoint for peer:{} already exist, rejecting connection req...", peerInfo);
            connectionReq.reject();
            return ep;
          }
        });
      }
    }

    @Override
    public void run() {
      UcpRequest reqToConn = recvEstablishConnRequest();
      while (!Thread.interrupted()) {
        try {
//          acceptNewConn();
          if (reqToConn.isCompleted()) {
            reqToConn = recvEstablishConnRequest();
          }
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