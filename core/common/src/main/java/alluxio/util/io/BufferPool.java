package alluxio.util.io;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledDirectByteBuf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BufferPool {
    private static final int KB = 1024;
    private static final int MB = KB * KB;

    private final int[] bufSize_ = {4*KB, 8*KB, 16*KB, 32*KB, 64*KB, 128*KB, 512*KB, 1*MB, 2*MB, 16*MB, 64*MB};
    // TOOD mem capped bounded q
    private ConcurrentLinkedQueue<ByteBuf>[] offHeapBuffers_ = new ConcurrentLinkedQueue[bufSize_.length];
    private ConcurrentLinkedQueue<ByteBuf>[] onHeapBuffers_ = new ConcurrentLinkedQueue[bufSize_.length];


    public BufferPool() {
        for (int i = 0; i < bufSize_.length; i++) {
            offHeapBuffers_[i] = new ConcurrentLinkedQueue<ByteBuf>();
            onHeapBuffers_[i] = new ConcurrentLinkedQueue<ByteBuf>();
        }
    }


    public ByteBuf getABuffer(int size, boolean isDirect) {
        ByteBuf retBuf = null;
        int idx = Arrays.binarySearch(bufSize_, size);
        if (idx >= bufSize_.length) {
            if (isDirect) {
                retBuf = Unpooled.directBuffer(size, size);
            } else {
                retBuf = Unpooled.buffer(size, size);
            }
            return retBuf;
        }
        if (idx < 0)
            idx = -idx-1;
        if (isDirect) {
            retBuf = offHeapBuffers_[idx].poll();
        } else {
            retBuf = onHeapBuffers_[idx].poll();
        }
        if (retBuf != null)
            return retBuf;
        if (isDirect) {
            retBuf = Unpooled.directBuffer(bufSize_[idx], bufSize_[idx]);
        } else {
            retBuf = Unpooled.buffer(bufSize_[idx], bufSize_[idx]);
        }
        return retBuf;
    }

    public void returnBuffer(ByteBuf buffer) {
        boolean isDirect = buffer.isDirect();
        int idx = Arrays.binarySearch(bufSize_, buffer.capacity());
        boolean fromPool = idx >= 0 && idx < bufSize_.length;
        if (!fromPool) {
            buffer.release();
        } else {
            if (isDirect) {
                offHeapBuffers_[idx].offer(buffer);
            } else {
                onHeapBuffers_[idx].offer(buffer);
            }
        }
    }

    public String printBufferStats() {
        StringBuilder sb = new StringBuilder();
        return sb.toString();
    }

    public static void main(String[] args)
    {
        try {
            BufferPool bufferPool = new BufferPool();
            int idx = Arrays.binarySearch(bufferPool.bufSize_, 16*KB + 1);
            System.out.println(idx);
            /*
            Scanner scanner = new Scanner(System.in);
            List<ByteBuf> bufferList = new ArrayList<>();
            while (true) {
                String nextCmd = scanner.nextLine();
                switch (nextCmd.toLowerCase()) {
                    case "alloc":
//                        ByteBuffer nioBuffer = ByteBuffer.allocateDirect((int)(64*MB));
                        for (int j=0;j<64;j++) {
                            ByteBuf buffer = Unpooled.directBuffer((int) (4 * MB));
                            System.out.println("addr:" + buffer.memoryAddress());
                            for (int i = 0; i < 4 * MB; i += Long.BYTES)
                                buffer.writeLong(0L);
                            bufferList.add(buffer);
                        }
                        break;
                    case "clean":
                        for (ByteBuf buffer : bufferList) {
                            ByteBuffer internalBuf = buffer.nioBuffer();
                            BufferUtils.cleanDirectBuffer(internalBuf);
                            System.out.println("=> cleaned.");
                        }
                        break;
                    case "release":
                        for (ByteBuf buffer : bufferList) {
                            if (buffer != null) {
                                boolean released = buffer.release();
                                buffer = null;
                                System.out.println("=> released : " + released);
                            }
                        }
                        bufferList.clear();
                        break;
                    case "gc":
                        System.out.printf("Memory used before %,d MB%n", mbUsed());
                        System.gc();
                        System.out.printf("Memory used after %,d MB%n", mbUsed());
                        break;
                    case "exit":
                        return;
                    default:
                        System.out.println("Unknown.");
                        break;
                }
            }
            */
        } catch (Throwable th)
        {
            System.out.println(th.getMessage());
        }
    }

    private static long mbUsed() {
        return (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/1024/1024;
    }
}
