package alluxio.client.file.dora.ucx;

import alluxio.PositionReader;
import alluxio.file.ByteBufferTargetBuffer;
import alluxio.file.ReadTargetBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.ucx.UcpUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.ucx.UcpProxy;

import com.google.common.base.Preconditions;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

public class UcxDataReader implements PositionReader {
  private static final Logger LOG = LoggerFactory.getLogger(UcxDataReader.class);
  public static final int PAGE_SIZE = 4096;

  InetSocketAddress mAddr;
  private static InetSocketAddress sLocalAddr = null;
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

  public void acquireServerConn() {
    if (mWorkerEndpoint != null) {
      return;
    }
    LOG.info("Acquiring server connection for {}", mAddr);
    mWorkerEndpoint = mWorker.newEndpoint(
        new UcpEndpointParams()
            .setPeerErrorHandlingMode()
            .setErrorHandler((ep, status, errorMsg) ->
                LOG.error("[ERROR] creating ep to remote:"
                    + mAddr + " errored out: " + errorMsg
                    + " status:" + status + ",ep:" + ep.toString()))
            .setSocketAddress(mAddr));
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
    Protocol.ReadRequest.Builder builder = mRequestBuilder.get()
        .setLength(length)
        .setOffset(position)
        .clearCancel();
    Protocol.ReadRequest readRequest = builder.build();
    byte[] serializedBytes = readRequest.toByteArray();
    ByteBuffer buf = ByteBuffer.allocateDirect(PAGE_SIZE);
    buf.put(serializedBytes);
    buf.rewind();
    long tag = UcpUtils.generateTag(sLocalAddr);
    UcpRequest sendRequest = mWorkerEndpoint.sendTaggedNonBlocking(buf, tag, new UcxCallback() {
      public void onSuccess(UcpRequest request) {
        LOG.info("ReadReq:{} sent.", readRequest);
      }

      public void onError(int ucsStatus, String errorMsg) {
        throw new UcxException(errorMsg);
      }
    });
    waitForRequest(sendRequest);
    // now wait to recv data
    Preconditions.checkArgument((buffer instanceof ByteBufferTargetBuffer
            && buffer.byteBuffer().isDirect()),
        "Must be ByteBufferTargetBuffer with direct ByteBuffer");
    UcpRequest recvRequest = mWorker.recvTaggedNonBlocking(
        UcxUtils.getAddress(buffer.byteBuffer()), length, 0,0, null);
    waitForRequest(recvRequest);
    return 0;
  }
}
