package alluxio.util.io;

import com.google.common.base.Preconditions;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

public class ByteBufferOutputStream extends DataOutputStream {

  public static class InternalOutputStream extends OutputStream {
    private final ByteBuffer mBuffer;

    public InternalOutputStream(ByteBuffer buffer) {
      Preconditions.checkArgument(buffer.isDirect(), "Input bytebuffer must be direct.");
      mBuffer = buffer.duplicate();
    }

    @Override
    public void write(int b) throws IOException {
      try {
        mBuffer.put((byte) b);
      } catch (BufferOverflowException | ReadOnlyBufferException ex) {
        throw new IOException(ex);
      }
    }

    @Override
    public void close() throws IOException {
      // DO NOTHING
    }
  }

  /**
   * Assumption here is this buffer got its writable window adjusted already
   * when calling this func.
   * @param buffer
   * @return
   */
  public static ByteBufferOutputStream getOutputStream(ByteBuffer buffer) {
    ByteBufferOutputStream.InternalOutputStream internalOs =
        new ByteBufferOutputStream.InternalOutputStream(buffer);
    return new ByteBufferOutputStream(internalOs);
  }

  public ByteBufferOutputStream(ByteBufferOutputStream.InternalOutputStream internalOs) {
    super(internalOs);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
