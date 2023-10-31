package alluxio.worker.ucx;

import io.netty.buffer.ByteBuf;
import org.openucx.jucx.ucp.UcpWorker;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class AlluxioUcxUtils {
  public static final int METADATA_SIZE_COMMON = 4096;

  public static void writeConnectionMetadata(
      long tagToSend,
      long tagToReceive,
      ByteBuffer targetBuffer,
      UcpWorker localWorker) {
    // long(tag to send) | long (tag to receive) | int (worker addr size) | bytes (worker addr)
    // we allocate the common metadata size to match the send/recv tag exchange size
    targetBuffer.putLong(tagToSend);
    targetBuffer.putLong(tagToReceive);
    ByteBuffer localWorkerAddr = localWorker.getAddress();
    targetBuffer.putInt(localWorkerAddr.capacity()); // UcpWorer.getAddress always return a buffer with full capacity filled
    targetBuffer.put(localWorkerAddr);
    targetBuffer.clear();
  }
}
