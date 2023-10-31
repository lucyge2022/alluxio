package alluxio.worker.ucx;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointErrorHandler;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucs.UcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handle all ucx connection related logics.
 * Connection establish / Disconnect / Error Handling etc.
 */
public class UcxConnection {
  private static final Logger LOG = LoggerFactory.getLogger(UcxConnection.class);
  private long mTagToReceive;
  private long mTagToSend;
  private UcpEndpoint mEndpoint;
  private InetSocketAddress mRemoteAddress;
  // tag 0 is always reserved for general metadata exchange.
  private static final AtomicLong mTagGenerator = new AtomicLong(1L);
  // UcxConn to its own counter (for active msg or other usages... keep as a placeholder for now)
  private static final ConcurrentHashMap<UcxConnection, Set<ActiveRequest>>
      mRemoteConnections = new ConcurrentHashMap<>();
  // For streaming feature so we can send out-of-order data for multiplexing
  private AtomicLong mSequencer = new AtomicLong(1L);

  public UcxConnection() {
  }

  public long getTagToSend() {
    return mTagToSend;
  }

  public void setTagToSend(long tagToSend) {
    this.mTagToSend = tagToSend;
  }

  public long getTagToReceive() {
    return mTagToReceive;
  }

  public void setTagToReceive(long tagToReceive) {
    this.mTagToReceive = tagToReceive;
  }

  public UcpEndpoint getEndpoint() {
    return mEndpoint;
  }

  public void setEndpoint(UcpEndpoint mEndpoint) {
    this.mEndpoint = mEndpoint;
  }

  public InetSocketAddress getRemoteAddress() {
    return mRemoteAddress;
  }

  public void setRemoteAddress(InetSocketAddress mRemoteAddress) {
    this.mRemoteAddress = mRemoteAddress;
  }

  public long getNextSequence() {
    return mSequencer.incrementAndGet();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("UcpEndpoint", mEndpoint)
        .add("TagToSend", mTagToSend)
        .add("TagToReceive", mTagToReceive)
        .add("RemoteAddress", mRemoteAddress)
        .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof UcxConnection)) {
      return false;
    }
    UcxConnection otherConn = (UcxConnection) other;
    return Objects.equal(otherConn.getTagToReceive(), getTagToReceive())
        && Objects.equal(otherConn.getTagToSend(), getTagToSend());
  }

  public void startRecvRPCRequest() {
    Preconditions.checkNotNull(mEndpoint, "UcpEndpoint is null, this should not happen.");
    // create a bytebuffer wrapped and protected by UcpMemory
    // TODO(lucy) pool this UcpMem and reuse for next recvRpc,
    // coz transfer into msg will have its own copy of buffer.
    UcpMemory recvMemoryBlock =
        UcxMemoryPool.allocateMemory(AlluxioUcxUtils.METADATA_SIZE_COMMON,
        UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST);

    ActiveRequest activeRequest = new ActiveRequest();
    activeRequest.setUcpMemory(recvMemoryBlock);
    final UcxConnection thisConn = this;
    UcpRequest recvRequest = UcpServer.getInstance().getGlobalWorker().recvTaggedNonBlocking(
        recvMemoryBlock.getAddress(), recvMemoryBlock.getLength(),
        mTagToReceive, 0xFFFFFFFFFFFFL, new UcxCallback() {
          public void onSuccess(UcpRequest request) {
            LOG.info("New req received from peer:{}", mEndpoint);
            try {
              // this entire memory block is owned and registered by ucx
              ByteBuffer rpcRecvBuffer = UcxUtils.getByteBufferView(
                  recvMemoryBlock.getAddress(), recvMemoryBlock.getLength());
              UcxMessage msg = UcxMessage.fromByteBuffer(rpcRecvBuffer);
              mRemoteConnections.compute(thisConn, (conn, activeRequestSet) -> {
                if (activeRequestSet == null) {
                  return null;
                }
                try {
                  activeRequest.close();
                } catch (IOException e) {
                  // actually there won't be checked exception thrown.
                  LOG.error("Error of closing activeRequest:{}", activeRequest);
                }
                activeRequestSet.remove(activeRequest);
                return activeRequestSet;
              });
              UcxRequestHandler reqHandler = msg.getRPCMessageType().mHandlerSupplier.get();
              msg.getRPCMessageType().mStage.mThreadPool.execute(() -> {
                try {
                  reqHandler.handle(msg, thisConn);
                } catch (Throwable ex) {
                  LOG.error("Exception when handling req:{} from remote:{}",
                      msg, mEndpoint);
                }
              });
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            LOG.info("onSuccess start receiving another req for remote:{}", mEndpoint);
            startRecvRPCRequest();
          }

          public void onError(int ucsStatus, String errorMsg) {
            LOG.error("Receive req errored, status:{}, errMsg:{}",
                ucsStatus, errorMsg);
            LOG.info("onError start receiving another req for remote:{}", mEndpoint);
            startRecvRPCRequest();
          }
        });
    activeRequest.setUcpRequest(recvRequest);
    mRemoteConnections.compute(this, (conn, activeReqQ) -> {
      if (activeReqQ == null) {
        activeReqQ = new HashSet<>();
      }
      activeReqQ.add(activeRequest);
      return activeReqQ;
    });
  }

  public static class ActiveRequest implements Closeable {
    // pending ucprequest created on this particular UcxConnection
    private UcpRequest mUcpRequest;
    // The reference of pending memory allocated during creation of ucp requests
    // lifecycle of this memory block gets tracked and handled in here.
    private UcpMemory mMemoryBlock;

    public ActiveRequest() {
    }

    public void setUcpRequest(UcpRequest ucpRequest) {
      if (mUcpRequest == null) {
        mUcpRequest = ucpRequest;
      }
    }

    public void setUcpMemory(UcpMemory registerdMem) {
      if (mMemoryBlock == null) {
        mMemoryBlock = registerdMem;
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("UcpRequest status", mUcpRequest.getStatus())
          .add("UcpRequest recvSize", mUcpRequest.getRecvSize())
          .add("UcpRequest senderTag", mUcpRequest.getSenderTag())
          .add("UcpMemory length", mMemoryBlock.getLength())
          .toString();
    }

    @Override
    public void close() throws IOException {
      /* don't know how to close UcpRequest properly, it seems in recvTaggedNonBlockingNative
      jucx_request_allocate creates a new global ref of the newly created jucx_request(UcpRequest)
      to pass into ucp_request_param_t but never explicitly delete this global ref.
      Currently finding out reason or if there's a bug in OpenUcx community.
      */
      if (mUcpRequest != null) {
        UcpServer.getInstance().getGlobalWorker().cancelRequest(mUcpRequest);
      }
      if (mMemoryBlock != null) {
        mMemoryBlock.close();
      }
    }
  }

  static class UcxConnectionEstablishCallBack extends UcxCallback {
    private ByteBuffer mEstablishConnBuf;
    private UcpWorker mWorker;

    public UcxConnectionEstablishCallBack(ByteBuffer establishConnBuf, UcpWorker worker) {
      mEstablishConnBuf = establishConnBuf;
      mWorker = worker;
    }

    public void onSuccess(UcpRequest request) {
      LOG.info("onSuccess for new ConnectionEstablish req.");
      mEstablishConnBuf.clear();
      // long(tag for send) | long (tag for receive) | int (worker addr size) | bytes (worker addr)
      // check UcxUtils.buildConnectionMetadata for details
      long tagToSend = mEstablishConnBuf.getLong();
      long tagToReceive = mEstablishConnBuf.getLong();
      UcxConnection newConnection = new UcxConnection();
      newConnection.setTagToReceive(tagToReceive);
      newConnection.setTagToSend(tagToSend);


      int workerAddrSize = mEstablishConnBuf.getInt();
      ByteBuffer workerAddr = ByteBuffer.allocateDirect(workerAddrSize);
      mEstablishConnBuf.limit(mEstablishConnBuf.position() + workerAddrSize);
      workerAddr.put(mEstablishConnBuf);

      if (tagToSend == -1)
        tagToSend = mTagGenerator.incrementAndGet();
      UcpEndpoint clientEp = mWorker.newEndpoint(new UcpEndpointParams()
          .setErrorHandler(new UcxConnectionErrorHandler(newConnection))
          .setPeerErrorHandlingMode()
          .setUcpAddress(workerAddr));
      newConnection.setEndpoint(clientEp);
      newConnection.setTagToSend(tagToSend);
      newConnection.setTagToReceive(tagToReceive);
      // TODO(lucy) bail if there's already existing connection? shouldn't happen
      // tag send/recv is only unique to a one pair of connection
      // reject or do sth here.
      mRemoteConnections.putIfAbsent(newConnection, new HashSet<>());

      // Send my info with client
      UcpMemory recvMemoryBlock =
          UcxMemoryPool.allocateMemory(AlluxioUcxUtils.METADATA_SIZE_COMMON,
              UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST);
      AlluxioUcxUtils.writeConnectionMetadata(
          tagToReceive,
          tagToSend,
          UcxUtils.getByteBufferView(
              recvMemoryBlock.getAddress(), recvMemoryBlock.getLength()),
          mWorker);
      // acceptor thread will help progress
      clientEp.sendTaggedNonBlocking(recvMemoryBlock.getAddress(),
          recvMemoryBlock.getLength(), 0L, new UcxCallback() {
        public void onSuccess(UcpRequest request) {
          LOG.info("onSuccess in sending back metadata info to client:{},ucxconn:{}",
              clientEp, newConnection);
          recvMemoryBlock.close();
        }

        public void onError(int ucsStatus, String errorMsg) {
          LOG.error("onError in sending back metadata info to client:{},ucsStatus:{},errMsg:{}",
              clientEp, ucsStatus, errorMsg);
          recvMemoryBlock.close();
        }
      });
      LOG.info("Connection established with remote:{}, start recv-ing RPC request...",
          newConnection);
      newConnection.startRecvRPCRequest();
    }

    public void onError(int ucsStatus, String errorMsg) {
      LOG.error("onError for new ConnectionEstablish req.ucsStatus:{}:errMsg:{}",
          ucsStatus, errorMsg);
      throw new UcxException(errorMsg);
    }
  }

  static class UcxConnectionErrorHandler implements UcpEndpointErrorHandler {
    private final UcxConnection mUcxConnection;
    public UcxConnectionErrorHandler(UcxConnection ucxConnection) {
      mUcxConnection = ucxConnection;
    }

    @Override
    public void onError(UcpEndpoint errorHandlingEndpoint, int status, String errorMsg)
        throws Exception {
      LOG.warn("Error in connection:{}, closing related resources...", mUcxConnection);
      UcpEndpoint remoteEndpoint = mUcxConnection.getEndpoint();
      if (remoteEndpoint != null) {
        LOG.info("Closing remoteEp:{} on error, status:{}:errorMsg:{}", remoteEndpoint, status, errorMsg);
        remoteEndpoint.close();
      }
    }
  }


}
