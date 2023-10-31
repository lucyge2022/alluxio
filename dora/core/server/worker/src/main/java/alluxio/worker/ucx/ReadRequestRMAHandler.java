package alluxio.worker.ucx;

import alluxio.AlluxioURI;
import alluxio.client.file.cache.PageId;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.io.ByteBufferInputStream;
import alluxio.util.io.ByteBufferOutputStream;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
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
              }

              public void onError(int ucsStatus, String errorMsg) {
                LOG.error("onError put pageid:{}:pageOffset:{}:len:{}"
                        + " ucsStatus:{}:errMsg:{}",
                    pageId, pageOffset, readLen, ucsStatus, errorMsg);
//                throw new UcxException(errorMsg);
              }
            });
        requests.add(putRequest);
      } catch (PageNotFoundException | IOException e) {
        throw new RuntimeException(e);
      }
    } // end for
    LOG.info("Handle RMA read req:{} complete", readRequest);
    while (requests.stream().anyMatch(r -> !r.isCompleted())) {
      LOG.info("Wait for all {} ucpreq to complete, sleep for 5 sec...", requests.size());
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    LOG.info("All PUT request completed, notifying client...");
    Protocol.ReadResponseRMA readResponseRMA = Protocol.ReadResponseRMA.newBuilder()
        .setReadLength(bytesRead).build();
    byte[] responseBytes = readResponseRMA.toByteArray();
    UcxMessage replyMessage = new UcxMessage(message.getMessageId(),
        UcxMessage.Type.Reply,
        null); // reply -> Protocol.ReadResponseRMA
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
    UcpRequest req = remoteConnection.getEndpoint().sendTaggedNonBlocking(
        replyBuffer, remoteConnection.getTagToSend(), new UcxCallback() {
      public void onSuccess(UcpRequest request) {
        LOG.error("onSuccess");
      }

      public void onError(int ucsStatus, String errorMsg) {
        throw new UcxException(errorMsg);
      }
    });
    while (!req.isCompleted()) {
      LOG.info("Wait for RMA reply msg, sleep for 5 sec...", requests.size());
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
