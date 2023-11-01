package alluxio.worker.ucx;

import alluxio.AlluxioURI;
import alluxio.client.file.cache.PageId;
import alluxio.concurrent.jsr.CompletableFuture;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.runtime.UnknownRuntimeException;
import alluxio.proto.dataserver.Protocol;
import alluxio.ucx.AlluxioUcxUtils;
import alluxio.util.io.ByteBufferInputStream;
import alluxio.util.io.ByteBufferOutputStream;

import com.google.common.base.Preconditions;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpRemoteKey;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucs.UcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class ReadRequestRMAHandler implements UcxRequestHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ReadRequestRMAHandler.class);
  /* Current Work flow: client initiate RMAReadRequest msg,
  client prepare local mem region, send the rpc req thru tag API,
  server do PUT to client mem region.
   */

  @Override
  public void handle(UcxMessage message, UcxConnection remoteConnection) {
    UcpEndpoint remoteEp = remoteConnection.getEndpoint();
    ByteBuffer infoBuffer = message.getRPCMessage();
    long remoteMemAddress;
    UcpRemoteKey remoteRKey;
    Protocol.ReadRequest readRequest;
    try (ByteBufferInputStream bbis = ByteBufferInputStream.getInputStream(infoBuffer)) {
      //  RPC message contains:
      //  client remote mem addr (long) | client remote mem addr Rkey buffer
      //  ReadRequest protobuf
      LOG.info("[DEBUG], before read remote mem info, infobuffer capacity:{}:pos:{}:limit:{}.",
          infoBuffer.capacity(), infoBuffer.position(), infoBuffer.limit());
      remoteMemAddress = bbis.readLong();
      int rkeyBufferSize = infoBuffer.getInt();
      ByteBuffer rkeyBuf = ByteBuffer.allocateDirect(rkeyBufferSize);
      bbis.read(rkeyBuf, rkeyBufferSize);
      rkeyBuf.clear();
      remoteRKey = remoteEp.unpackRemoteKey(rkeyBuf);
      LOG.info("[DEBUG], after read remote mem info, infobuffer capacity:{}:pos:{}:limit:{}.",
          infoBuffer.capacity(), infoBuffer.position(), infoBuffer.limit());
      readRequest = Protocol.ReadRequest.parseFrom(infoBuffer.duplicate());
    } catch (IOException e) {
      LOG.error("Exception in parsing RMA Read Request:", e);
      throw new RuntimeException(e);
    }

    final String fileId =
        new AlluxioURI(readRequest.getOpenUfsBlockOptions().getUfsPath()).hash();
    long offset = readRequest.getOffset();
    long totalLength = readRequest.getLength();
    long pageSize = Configuration.global().getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);
    long remoteAddrPosition = remoteMemAddress;

    List<UcpRequest> requests = new ArrayList<>();
    AsyncFuture<String> asyncFuture = new AsyncFuture<>();
    int totalRequests = 0;
    int bytesRead = 0;
    for (; bytesRead < totalLength; ) {
      int pageIndex = (int)(offset / pageSize);
      int pageOffset = (int)(offset % pageSize);
      int readLen = (int)Math.min(totalLength - bytesRead, pageSize - pageOffset);
      PageId pageId = new PageId(fileId, pageIndex);
      try {
        Optional<UcpMemory> readContentUcpMem =
            UcpServer.getInstance().mlocalCacheManager.get(pageId, pageOffset, readLen);
        if (!readContentUcpMem.isPresent()) {
          break;
        }
        Preconditions.checkArgument(readLen == readContentUcpMem.get().getLength(),
            "readLen not equal to supplied UcpMemory length");
        offset += readLen;
        bytesRead += readLen;
        remoteAddrPosition += readLen;
        UcpRequest putRequest = remoteEp.putNonBlocking(readContentUcpMem.get().getAddress(),
            readContentUcpMem.get().getLength(), remoteAddrPosition,
            remoteRKey, new UcxCallback() {
              public void onSuccess(UcpRequest request) {
                LOG.info("onSuccess put pageid:{}:pageOffset:{}:len:{}",
                    pageId, pageOffset, readLen);
                asyncFuture.complete(String.format("pageid:%d:pageOffset:%d:len:%d",
                    pageId, pageOffset, readLen));
              }

              public void onError(int ucsStatus, String errorMsg) {
                LOG.error("onError put pageid:{}:pageOffset:{}:len:{}"
                        + " ucsStatus:{}:errMsg:{}",
                    pageId, pageOffset, readLen, ucsStatus, errorMsg);
                asyncFuture.fail(new UcxException(errorMsg));
              }
            });
        totalRequests += 1;
      } catch (PageNotFoundException | IOException e) {
        throw new RuntimeException(e);
      }
    } // end for
    LOG.info("Handle RMA read req:{} complete, blockingly wait for all RMA PUT to compelte",
        readRequest);
    asyncFuture.setTotalExpected(totalRequests);
    try {
      asyncFuture.get();
    } catch (ExecutionException | InterruptedException e) {
      throw new UnknownRuntimeException("Exception during waiting for RMA PUT to complete.");
    }

    LOG.info("All PUT request completed, notifying client...");
    Protocol.ReadResponseRMA readResponseRMA = Protocol.ReadResponseRMA.newBuilder()
        .setReadLength(bytesRead).build();
    byte[] responseBytes = readResponseRMA.toByteArray();
    UcxMessage replyMessage = new UcxMessage(message.getMessageId(),
        UcxMessage.Type.Reply,
        ByteBuffer.wrap(responseBytes)); // reply -> Protocol.ReadResponseRMA
    UcpMemory relyMemoryBlock = UcxMemoryPool.allocateMemory(AlluxioUcxUtils.METADATA_SIZE_COMMON,
        UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST);
    ByteBuffer replyBuffer = UcxUtils.getByteBufferView(relyMemoryBlock.getAddress(),
        relyMemoryBlock.getLength());
    try (ByteBufferOutputStream bbos = ByteBufferOutputStream.getOutputStream(replyBuffer)) {
      UcxMessage.toByteBuffer(replyMessage, bbos);
      replyBuffer.clear();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    CompletableFuture<Boolean> completed = new CompletableFuture<>();
    UcpRequest req = remoteConnection.getEndpoint().sendTaggedNonBlocking(
        replyBuffer, remoteConnection.getTagToSend(), new UcxCallback() {
      public void onSuccess(UcpRequest request) {
        LOG.error("onSuccess");
        completed.complete(true);
      }

      public void onError(int ucsStatus, String errorMsg) {
        completed.complete(false);
        throw new UcxException(errorMsg);
      }
    });
    LOG.info("Blockingly wait for completion reply msg sending...");
    boolean completeSending = false;
    try {
      completeSending = completed.get();
      if (!completeSending) {
        throw new UnknownRuntimeException(String.format("Error sending compeletion reply after handling ucxmsg:%s",
            message));
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new UnknownRuntimeException(String.format("Error sending compeletion reply after handling ucxmsg:%s",
          message));
    }
  }
}
