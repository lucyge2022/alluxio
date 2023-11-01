package alluxio.ucx;

import static java.nio.charset.StandardCharsets.UTF_8;
import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.LocalCacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.worker.ucx.UcxDataReader;
import alluxio.conf.Configuration;
import alluxio.proto.dataserver.Protocol;
//import alluxio.worker.ucx.UcpServer;

import org.apache.commons.codec.binary.Hex;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.function.Supplier;

public class TestUcxRead {

  public int serverPort = 1234;
  public String serverHost = "localhost";
//  public UcpServer mServer;
//  public UcpContext mContext;
  public LocalCacheManager mLocalCacheManager;
  private Random mRandom = new Random();

  @BeforeClass
  public void initServer() throws IOException {
//    mServer = new UcpServer();
//    mContext = new UcpContext(new UcpParams()
//        .requestStreamFeature()
//        .requestTagFeature()
//        .requestWakeupFeature());
    mLocalCacheManager = (LocalCacheManager) CacheManager.Factory.get(Configuration.global());
  }

  class SampleData {
    String mMd5;
    byte[] mData;
    public SampleData(byte[] data) {
      mData = data;
      try {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(data);
        mMd5 = Hex.encodeHexString(md.digest()).toLowerCase();
      } catch (NoSuchAlgorithmException e) {
        /* No actions. Continue with other hash method. */
      }
    }
  }

  public byte[] generateRandomData(int size) {
    byte[] bytes = new byte[size];
    mRandom.nextBytes(bytes);
    return bytes;
  }


  @Test
  public void testClientServer() {
    String dummyUfsPath = "hdfs://localhost:9000/randomUfsPath";
    int pageSize = 1024 * 1024;
    SampleData sampleData = new SampleData(generateRandomData(1024 * 1024));
    Supplier<byte[]> externalDataSupplier = () -> {
      return sampleData.mData;
    };
    int totalLen = 5 * pageSize;
    int totalPages = totalLen / pageSize;
    for (int i=0; i<totalPages; i++) {
      PageId pageId = new PageId(new AlluxioURI(dummyUfsPath).hash(), i);
      mLocalCacheManager.cache(pageId, CacheContext.defaults(), externalDataSupplier);
    }
    InetSocketAddress serverAddr = new InetSocketAddress(serverHost, serverPort);
//    UcpWorker clientWorker = mContext.newWorker(new UcpWorkerParams());

    Protocol.OpenUfsBlockOptions openUfsBlockOptions =
        Protocol.OpenUfsBlockOptions.newBuilder().setUfsPath(dummyUfsPath)
            .setOffsetInFile(0).setBlockSize(totalLen)
//            .setMaxUfsReadConcurrency(mergedOptions.getMaxUfsReadConcurrency())
            .setNoCache(true)
            .setMountId(0)
            .build();

    for (int i=0; i<totalPages; i++) {
      long position = i * pageSize;
      int length = pageSize;
      Protocol.ReadRequest.Builder requestBuilder = Protocol.ReadRequest.newBuilder()
          .setOpenUfsBlockOptions(openUfsBlockOptions);
      UcxDataReader reader = new UcxDataReader(serverAddr, null, requestBuilder);
      ByteBuffer buffer = ByteBuffer.allocateDirect(length);
      try {
        reader.read(position, buffer, length);
      } catch (IOException e) {
        System.out.println("IOException on position:" + position + ":length:" + length);
        throw new RuntimeException(e);
      }
    }
  }
}
