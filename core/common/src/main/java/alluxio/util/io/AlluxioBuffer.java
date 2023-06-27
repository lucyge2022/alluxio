package alluxio.util.io;


import io.netty.buffer.ByteBuf;
import org.checkerframework.checker.units.qual.A;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntBinaryOperator;

public class AlluxioBuffer implements Closeable {
  private AtomicReference<ByteBuf> mBuffer = new AtomicReference<>();
  private AtomicBoolean mClosed = new AtomicBoolean(false);
  private int mLength;
  private AtomicInteger mRefCnt = new AtomicInteger(0);
  private static final RefCountOperator mOperator = new RefCountOperator();
  private Optional<AlluxioBuffer> mParentBuffer = Optional.empty();

  public static AlluxioBuffer allocate(int length, boolean isDirect) {
    return new AlluxioBuffer(length, isDirect);
  }

  private AlluxioBuffer(int length, boolean isDirect) {
    mLength = length;
    mBuffer.set(BufferPool.getInstance().getABuffer(length, isDirect));
    mRefCnt.accumulateAndGet(1, mOperator);
  }

  /**
   * Constructor for child buffer, an independent read-only buffer
   * with an internal non-retained ByteBuf's duplicate(ByteBuf.duplicate)
   * serving for reads.
   * @param parentBuffer
   */
  private AlluxioBuffer(AlluxioBuffer parentBuffer) {
    mParentBuffer = Optional.of(parentBuffer);
    mBuffer.set(parentBuffer.mBuffer.get().duplicate());
  }

  public int length() {
    if (mParentBuffer.isPresent()) {
      return mParentBuffer.get().length();
    }
    return mLength;
  }

  public int capacity() {
    if (mParentBuffer.isPresent()) {
      return mParentBuffer.get().capacity();
    }
    return mBuffer.get().capacity();
  }

  /**
   * Create a child buffer of this AlluxioBuffer for reads,
   * if retainDuplicate is called upon a child AlluxioBuffer is redirected
   * to call retainDuplicate on its parent buffer. No ref cnt increment will
   * happen for a child buffer.
   * Retain -> increment the refcnt, and
   * Duplicate -> create a duplicate of this AlluxioBuffer to read
   * @return a child AlluxioBuffer backed with a parent AlluxioBuffer
   */
  public AlluxioBuffer retainDuplicate() {
    // I'm a child buffer, redirect to call my parent buffer's retainDuplicate
    if (mParentBuffer.isPresent()) {
      return mParentBuffer.get().retainDuplicate();
    }
    // I'm a parent buffer, make a child buffer for a independent read-only buffer to consume.
    if (mRefCnt.accumulateAndGet(1, mOperator) > 0) {
      return new AlluxioBuffer(this);
    }
    // I'm a parent buffer and I'm already closed and returned to pool
    return null;
  }

  /**
   * Transfer this AlluxioBuffer's data into to the specified destination
   * @param dst destination byte array to tranfer data into
   * @param offset offset within dst bytearray to transfer data into
   * @param length length of bytes to transfer into
   */
  public void get(byte[] dst, int offset, int length) {
    mBuffer.get().readBytes(dst, offset, Math.min(readableBytes(), length));
  }

  public boolean isClosed() {
    return mClosed.get();
  }

  @Override
  public void close() throws IOException {
    if (!mClosed.get()) {
      return;
    }
    if (mParentBuffer.isPresent()) {
      mParentBuffer.get().close();
      return;
    }
    if (mRefCnt.accumulateAndGet(-1, mOperator) == -1
        && mClosed.compareAndSet(false, true)) {
      if (mParentBuffer.isPresent()) {
        mParentBuffer.get().close();
      } else {
        BufferPool.getInstance().returnBuffer(mBuffer.get());
      }
    }
  }

  public void setReadIndex(int index) {
    mBuffer.get().readerIndex(index);
  }

  public void setWriteIndex(int index) throws IOException {
    if (mParentBuffer.isPresent()) {
      throw new IOException("Child AlluxioBuffer only for read now, writer index set disallowed.");
    }
    mBuffer.get().writerIndex(index);
  }

  public int readableBytes() {
    return mBuffer.get().readableBytes();
  }

  public int writableBytes() {
    return mBuffer.get().writableBytes();
  }

  /**
   * fill me for a maximum of @param length from given input stream.
   * @param in
   * @param length
   * @return actual length transferred from InputStream to this buffer
   * @throws IOException
   */
  public int fillMe(InputStream in, int length) throws IOException {
    if (mParentBuffer.isPresent()) {
      throw new IOException("Write not allowed for child AlluxioBuffer.");
    }
    if (length < 0) {
      throw new IllegalArgumentException("Length must not be negative: " + length);
    }
    int remaining = length;
    while (remaining > 0) {
      // writeBytes will automatically expand if not enough space, we can't let that happen.
      final int readCount = mBuffer.get().writeBytes(in, Math.min(writableBytes(), remaining));
      if (readCount == -1) { // EOF
        break;
      }
      remaining -= readCount;
    }
    return length - remaining;
  }

  public static class RefCountOperator implements IntBinaryOperator {

    @Override
    public int applyAsInt(int val, int add) {
      if (val == -1) {
        return -1;
      }
      if (val + add == 0) {
        return -1;
      }
      return val + add;
    }
  }
}
