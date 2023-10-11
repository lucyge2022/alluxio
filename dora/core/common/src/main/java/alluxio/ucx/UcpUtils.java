package alluxio.ucx;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class UcpUtils {

  public static long generateTag(InetSocketAddress inetSocketAddress) {
    // first 16 empty, then ipv4 = 8*4 = 32 bit then port = 16 bit
    long tag = 0L;
    InetAddress remoteAddr = inetSocketAddress.getAddress();
    if (remoteAddr instanceof Inet4Address) {
      byte[] ip = ((Inet4Address)remoteAddr).getAddress();
      tag |= ((long)0xFF & ip[0]) << 24;
      tag |= ((long)0xFF & ip[1]) << 16;
      tag |= ((long)0xFF & ip[2]) << 8;
      tag |= (long)0xFF & ip[3];
      tag = tag << 16;
    }
    tag |= inetSocketAddress.getPort();
    return tag;
  }
}
