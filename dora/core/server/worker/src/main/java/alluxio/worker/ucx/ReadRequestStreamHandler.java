package alluxio.worker.ucx;

import alluxio.AlluxioURI;
import alluxio.client.file.cache.PageId;
import alluxio.exception.PageNotFoundException;
import alluxio.proto.dataserver.Protocol;

import com.google.protobuf.InvalidProtocolBufferException;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class ReadRequestStreamHandler implements UcxRequestHandler {
  private static final Logger LOG = LoggerFactory.getLogger(UcpServer.class);
  private static final long WORKER_PAGE_SIZE = 1*1024*1024L;
  Protocol.ReadRequest mReadRequest = null;
  UcpEndpoint mRemoteEp;
  AtomicLong mSequencer;
  //conf.getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);

  public ReadRequestStreamHandler() {

  }

  public ReadRequestStreamHandler(UcpEndpoint remoteEndpoint, AtomicLong sequencer,
                                  Protocol.ReadRequest request) {
    mReadRequest = null;
    mRemoteEp = remoteEndpoint;
    mSequencer = sequencer;
//    sequencer = mPeerToSequencers.computeIfAbsent(peerInfo, pi -> new AtomicLong(0L));
//      mReadRequest = parseReadRequest(recvBuffer);
//    mReadRequest = request;
  }

  @Override
  public void handle(UcxMessage message, UcpEndpoint remoteEndpoint) {
    mRemoteEp = remoteEndpoint;
    try {
      mReadRequest = Protocol.ReadRequest.parseFrom(message.getRPCMessage());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    final String fileId =
        new AlluxioURI(mReadRequest.getOpenUfsBlockOptions().getUfsPath()).hash();
    long offset = mReadRequest.getOffset();
    long totalLength = mReadRequest.getLength();
    List<UcpRequest> requests = new ArrayList<>();
    for (int bytesRead = 0; bytesRead < totalLength; ) {
      int pageIndex = (int)(offset / WORKER_PAGE_SIZE);
      int pageOffset = (int)(offset % WORKER_PAGE_SIZE);
      int readLen = (int)Math.min(totalLength - bytesRead, WORKER_PAGE_SIZE - pageOffset);
      PageId pageId = new PageId(fileId, pageIndex);
      try {
        Optional<UcpMemory> readContentUcpMem =
            UcpServer.getInstance().mlocalCacheManager.get(pageId, pageOffset, readLen);
        if (!readContentUcpMem.isPresent()) {
          break;
        }
        offset += readLen;
        bytesRead += readLen;
        // first 8 bytes -> sequence  second 8 bytes -> size
        ByteBuffer preamble = ByteBuffer.allocateDirect(16);
        preamble.clear();
        long seq = mSequencer.incrementAndGet();
        preamble.putLong(seq);
        preamble.putLong(readContentUcpMem.get().getLength());
        preamble.clear();
        UcpRequest preambleReq = mRemoteEp.sendStreamNonBlocking(UcxUtils.getAddress(preamble),
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
        UcpRequest sendReq = mRemoteEp.sendStreamNonBlocking(
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
    LOG.info("Handle read req:{} complete", mReadRequest);
    while (requests.stream().anyMatch(r -> !r.isCompleted())) {
      LOG.info("Wait for all {} ucpreq to complete, sleep for 5 sec...");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
