package alluxio.worker.ucx;

import alluxio.PositionReader;
import alluxio.file.ByteBufferTargetBuffer;
import alluxio.file.ReadTargetBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.ucx.AlluxioUcxUtils;
import alluxio.util.io.ByteBufferOutputStream;
import alluxio.worker.ucx.UcxConnection;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.ucx.UcpProxy;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpConstants;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucs.UcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

public class UcxDataReader implements PositionReader {
  private static final Logger LOG = LoggerFactory.getLogger(UcxDataReader.class);
  public static final int PAGE_SIZE = 4096;

  InetSocketAddress mAddr;
  private static InetSocketAddress sLocalAddr = null;

  UcxConnection mConnection;
  // make this a global, one per process only instance
  UcpWorker mWorker;
  UcpEndpoint mWorkerEndpoint;

  Supplier<Protocol.ReadRequest.Builder> mRequestBuilder;
  public UcxDataReader(InetSocketAddress addr, UcpWorker worker,
                       Protocol.ReadRequest.Builder requestBuilder) {
    try {
      sLocalAddr = new InetSocketAddress(InetAddress.getLocalHost(),0);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
    mAddr = addr;
    mWorker = worker;
    mRequestBuilder = requestBuilder::clone;
  }


  public void acquireServerConn() throws IOException {
    try {
      mConnection = UcxConnection.initNewConnection(mAddr, mWorker);
    } catch (Exception e) {
      throw new IOException(
          String.format("Error initializing conn with remote:%s", mAddr), e);
    }
  }

  synchronized public int progressWorker() throws Exception {
    return mWorker.progress();
  }

  public void waitForRequest(UcpRequest ucpRequest) {
    while(!ucpRequest.isCompleted()) {
      try {
        progressWorker();
      } catch (Exception e) {
        LOG.error("Error progressing req:", e);
      }
    }
  }

  @Override
  public int readInternal(long position, ReadTargetBuffer buffer, int length) throws IOException {
    // use Stream API
//    return readInternalStream(position, buffer, length);
    // use RMA API
    return readInternalRMA(position, buffer, length);
  }

  public int readInternalRMA(long position, ReadTargetBuffer buffer, int length)
      throws IOException {
    Preconditions.checkArgument(buffer.byteBuffer().isDirect(), "ByteBuffer must be direct buffer");
    Preconditions.checkArgument(buffer.byteBuffer().position() == 0,
        "Provided ByteBuffer holder has to be empty");
    // register this result memory buffer, pack this memory region info and send read req over.
    UcpMemory resultMemBlock = UcxMemoryPool.registerMemory(
        UcxUtils.getAddress(buffer.byteBuffer()), length);
    // pack to rkey buf
    ByteBuffer rkeyBuf = resultMemBlock.getRemoteKeyBuffer();
    Protocol.ReadRequest.Builder builder = mRequestBuilder.get()
        .setLength(length)
        .setOffset(position)
        .clearCancel();
    Protocol.ReadRequest readRequest = builder.build();
    byte[] readReqBytes = readRequest.toByteArray();
    ByteBuffer rpcMesgBuf = ByteBuffer.allocate(
        Long.BYTES +  // local result mem addr
        Integer.BYTES +  // length of rkey
        rkeyBuf.remaining() + // rkey
        readReqBytes.length); // ReadRequest protobuf content
    try (ByteBufferOutputStream bbos = ByteBufferOutputStream.getOutputStream(rpcMesgBuf)) {
      bbos.writeLong(resultMemBlock.getAddress());
      bbos.writeInt(rkeyBuf.remaining());
      bbos.write(rkeyBuf, rkeyBuf.remaining());
      bbos.write(readReqBytes);
      rpcMesgBuf.clear();
    }
    UcxMessage readRMAMessage = new UcxMessage(0, UcxMessage.Type.ReadRMARequest,
        rpcMesgBuf);
    UcpMemory ucxMesgMem = UcxMemoryPool.allocateMemory(
        AlluxioUcxUtils.METADATA_SIZE_COMMON,
        UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST);
    ByteBuffer ucxMesgMemBuffer = UcxUtils.getByteBufferView(ucxMesgMem.getAddress(), ucxMesgMem.getLength());
    try (ByteBufferOutputStream bbos = ByteBufferOutputStream.getOutputStream(ucxMesgMemBuffer)) {
      UcxMessage.toByteBuffer(readRMAMessage, bbos);
    }
    ucxMesgMemBuffer.clear();
    UcpRequest sendRequest = mWorkerEndpoint.sendTaggedNonBlocking(
        ucxMesgMemBuffer, mConnection.getTagToSend(), new UcxCallback() {
          public void onSuccess(UcpRequest request) {
            LOG.info("ReadRMARequest:{} sent.", readRequest);
            ucxMesgMem.deregister();
          }

          public void onError(int ucsStatus, String errorMsg) {
            ucxMesgMem.deregister();
            throw new UcxException(errorMsg);
          }
        });
    LOG.info("Waiting for read request to send...");
    waitForRequest(sendRequest);

    // read with RMA
    int bytesRead = 0;
    UcpMemory relyMemoryBlock = UcxMemoryPool.allocateMemory(
        AlluxioUcxUtils.METADATA_SIZE_COMMON,
        UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST);
    UcpRequest replyReq = mConnection.getEndpoint().recvStreamNonBlocking(relyMemoryBlock.getAddress(),
        relyMemoryBlock.getLength(), 0, null);
    waitForRequest(replyReq);
    relyMemoryBlock.deregister();
    resultMemBlock.deregister(); // now target buffer available for caller to access
    return 0;
  }

  public int readInternalStream(long position, ReadTargetBuffer buffer, int length)
      throws IOException {
    Protocol.ReadRequest.Builder builder = mRequestBuilder.get()
        .setLength(length)
        .setOffset(position)
        .clearCancel();
    Protocol.ReadRequest readRequest = builder.build();
    byte[] serializedBytes = readRequest.toByteArray();
    ByteBuffer buf = ByteBuffer.allocateDirect(PAGE_SIZE);
    buf.putInt(serializedBytes.length);
    buf.put(serializedBytes);
    buf.clear();
    UcpRequest sendRequest = mWorkerEndpoint.sendTaggedNonBlocking(
        buf, mConnection.getTagToSend(), new UcxCallback() {
      public void onSuccess(UcpRequest request) {
        LOG.info("ReadReq:{} sent.", readRequest);
      }

      public void onError(int ucsStatus, String errorMsg) {
        throw new UcxException(errorMsg);
      }
    });
    LOG.info("Waiting for read request to send...");
    waitForRequest(sendRequest);
    // now wait to recv data
    Preconditions.checkArgument(buffer.byteBuffer().isDirect(), "ByteBuffer must be direct buffer");
    int bytesRead = 0;
    ByteBuffer preamble = ByteBuffer.allocateDirect(16);
    TreeMap<Long, ByteBuffer> buffers = new TreeMap<>();
    preamble.clear();
    LinkedList<UcpRequest> dataUcpRecvReqs = new LinkedList<>();
    while (bytesRead < length) {
      UcpRequest recvReq = mWorkerEndpoint.recvStreamNonBlocking(UcxUtils.getAddress(preamble), 16,
          UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, new UcxCallback() {
            public void onSuccess(UcpRequest request) {}

            public void onError(int ucsStatus, String errorMsg) {
              throw new UcxException(errorMsg);
            }
          });
      LOG.info("Waiting for preamble...");
      waitForRequest(recvReq);
      preamble.clear();
      long seq = preamble.getLong();
      long size = preamble.getLong();
      preamble.clear();
      ByteBuffer seqBuffer = ByteBuffer.allocateDirect(8);
      ByteBuffer dataBuffer = ByteBuffer.allocateDirect((int)size);
      long[] addrs = new long[2];
      long[] sizes = new long[2];
      addrs[0] = UcxUtils.getAddress(seqBuffer);
      addrs[1] = UcxUtils.getAddress(dataBuffer);
      sizes[0] = 8;
      sizes[1] = size;
      LOG.info("preamble info:seq:{}:len:{}", seq, size);
      UcpRequest dataRecvReq = mWorkerEndpoint.recvStreamNonBlocking(addrs, sizes,
          UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, new UcxCallback() {
            public void onSuccess(UcpRequest request) {
              ByteBuffer seqBufView = UcxUtils.getByteBufferView(addrs[0], sizes[0]);
              seqBufView.clear();
              long sequence = seqBufView.getLong();
              ByteBuffer dataBufView = UcxUtils.getByteBufferView(addrs[1], sizes[1]);
              dataBufView.clear();
              LOG.info("Received buffers, seq:{}, data buf size:{}", sequence, sizes[1]);
              buffers.put(sequence, dataBufView);
            }

            public void onError(int ucsStatus, String errorMsg) {
              LOG.error("Error receiving buffers, seq:{}, data buf size:{}, errorMsg:{}",
                  seq, size, errorMsg);
              throw new UcxException(errorMsg);
            }
          });
      LOG.info("Offering actual data recReq to q...");
      dataUcpRecvReqs.offer(dataRecvReq);
//      waitForRequest(recvReq);
      bytesRead += size;
    }
    while(!dataUcpRecvReqs.isEmpty()) {
      UcpRequest nextReq = dataUcpRecvReqs.poll();
      waitForRequest(nextReq);
    }
    buffer.byteBuffer().clear();
    while (!buffers.isEmpty()) {
      Map.Entry<Long, ByteBuffer> entry = buffers.pollFirstEntry();
      LOG.info("Copying seq:{},bufsize:{}", entry.getKey(), entry.getValue());
      entry.getValue().clear();
      buffer.byteBuffer().put(entry.getValue());
    }
    return 0;
  }
}
