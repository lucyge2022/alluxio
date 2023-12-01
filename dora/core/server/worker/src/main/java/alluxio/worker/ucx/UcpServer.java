package alluxio.worker.ucx;

import alluxio.AlluxioURI;
import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.LocalCacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.conf.Configuration;
import alluxio.util.ThreadFactoryUtils;

import org.apache.commons.codec.binary.Hex;
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

import alluxio.worker.dora.PagedDoraWorker;
import alluxio.worker.ucx.UcxConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class UcpServer {
  private static final Logger LOG = LoggerFactory.getLogger(UcpServer.class);

  public static UcpServer sInstance = null;
  public static ReentrantLock sInstanceLock = new ReentrantLock();
  public static final UcpContext sGlobalContext = new UcpContext(new UcpParams()
      .requestStreamFeature()
      .requestTagFeature()
      .requestRmaFeature()
      .requestWakeupFeature());
  public static int BIND_PORT = 1234;

  private UcpWorker mGlobalWorker;
  private Map<PeerInfo, UcpEndpoint> mPeerEndpoints = new ConcurrentHashMap<>();
  private Map<PeerInfo, AtomicLong> mPeerToSequencers = new ConcurrentHashMap<>();
  public CacheManager mCacheManager;
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
    return getInstance(null);
  }

  public static UcpServer getInstance(@Nullable Supplier<UcpServer> ucpServerSupplier) {
    if (sInstance != null)
      return sInstance;
    sInstanceLock.lock();
    try {
      if (sInstance != null)
        return sInstance;
      try {
        if (ucpServerSupplier != null) {
          sInstance = ucpServerSupplier.get();
        } else {
          sInstance = new UcpServer(null);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return sInstance;
    } finally {
      sInstanceLock.unlock();
    }
  }

  /*
   TODO for testing purpose without dependency on worker to provide
   cache manager capability, ufs path is local file path for now
   */
  private final static int  CACHE_MANAGER_PAGE_SIZE = 1024*1024;
  public void prefill(String ufsPath, int fileSize) {
    int offset = 0;
    while (offset < fileSize) {
      long pageIdx = offset / CACHE_MANAGER_PAGE_SIZE;
      PageId pageId = new PageId(new AlluxioURI(ufsPath).hash(), pageIdx);
      int bytesToCache = Math.min(CACHE_MANAGER_PAGE_SIZE, fileSize - offset);
      final int pos = offset;
      Supplier<byte[]> externalDataSupplier = () -> {
        byte[] bytes = new byte[bytesToCache];
        try (RandomAccessFile file = new RandomAccessFile(new AlluxioURI(ufsPath).getPath(), "r")) {
          file.seek(pos);
          file.read(bytes);
          return bytes;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };
      int cached = mCacheManager.cache(pageId, CacheContext.defaults(), externalDataSupplier);
      offset += cached;
    }
  }

  public UcpServer(@Nullable CacheManager cacheManager) throws IOException {
    if (cacheManager == null) {
      CacheManagerOptions cacheManagerOptions =
          CacheManagerOptions.createForWorker(Configuration.global());
      mCacheManager = LocalCacheManager.create(
          cacheManagerOptions, PageMetaStore.create(
              CacheManagerOptions.createForWorker(Configuration.global())));
    } else {
      mCacheManager = cacheManager;
    }
    mGlobalWorker = sGlobalContext.newWorker(new UcpWorkerParams()
        .requestWakeupRMA()
        .requestThreadSafety());
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
    LOG.info("Starting ucp server...");
    UcpServer ucpServer = UcpServer.getInstance();
    String basePath = args[1];
    int numOfFile = Integer.parseInt(args[2]);
    String ufsBase = "file://" + basePath;
    for (int i=1;i<=numOfFile;i++) {
      ucpServer.prefill(String.format("%s/file%d", ufsBase, i), 1024*1024);
    }
    LOG.info("Awaiting termination...");
    ucpServer.awaitTermination();
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

    public void acceptNewConn() {
      UcpConnectionRequest connectionReq = mConnectionRequests.poll();
      if (connectionReq != null) {
        try {
          UcpEndpoint bootstrapEp = mGlobalWorker.newEndpoint(new UcpEndpointParams()
              .setPeerErrorHandlingMode()
              .setConnectionRequest(connectionReq));
          UcxConnection ucxConnection = UcxConnection.acceptIncomingConnection(
              bootstrapEp, mGlobalWorker, connectionReq.getClientAddress());
          ucxConnection.startRecvRPCRequest();
        } catch (Exception e) {
          LOG.error("Error in acceptNewConn:", e);
        }
      }
    }

    @Override
    public void run() {
//      UcpRequest reqToConn = recvEstablishConnRequest();
      while (!Thread.interrupted()) {
        try {
          acceptNewConn();
//          if (reqToConn.isCompleted()) {
//            reqToConn = recvEstablishConnRequest();
//          }
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