package alluxio.worker.ucx;

import alluxio.AlluxioURI;
import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.LocalCacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.client.file.dora.ucx.UcxDataReader;
import alluxio.conf.Configuration;
import alluxio.proto.dataserver.Protocol;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Hex;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Supplier;

public class UcpClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(UcpClientTest.class);
  private static final UcpContext sGlobalContext = new UcpContext(new UcpParams()
      .requestStreamFeature()
      .requestTagFeature()
      .requestWakeupFeature());

  public Random mRandom = new Random();
  public LocalCacheManager mLocalCacheManager;
  public UcpWorker mWorker;

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

  public UcpClientTest() throws IOException {
    CacheManagerOptions cacheManagerOptions =
        CacheManagerOptions.createForWorker(Configuration.global());
    mLocalCacheManager = LocalCacheManager.create(
        cacheManagerOptions, PageMetaStore.create(
            CacheManagerOptions.createForWorker(Configuration.global())));
    mWorker = sGlobalContext.newWorker(new UcpWorkerParams());
  }

  public byte[] generateRandomData(int size) {
    byte[] bytes = new byte[size];
    mRandom.nextBytes(bytes);
    return bytes;
  }


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
    InetSocketAddress serverAddr = new InetSocketAddress("localhost", 1234);

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
      UcxDataReader reader = new UcxDataReader(serverAddr, mWorker, requestBuilder);
      ByteBuffer buffer = ByteBuffer.allocateDirect(length);
      try {
        reader.read(position, buffer, length);
        buffer.clear();
        byte[] readContent = new byte[length];
        buffer.get(readContent);
        Preconditions.checkArgument(Arrays.equals(readContent, sampleData.mData),
            String.format("pageid:{} content mismatch.", i));
      } catch (IOException e) {
        System.out.println("IOException on position:" + position + ":length:" + length);
        throw new RuntimeException(e);
      }
    }
  }

  public static void main(String[] args) {
    try {
      LOG.info("Instantiating UcpClientTest...");
      UcpClientTest ucpClientTest = new UcpClientTest();
      LOG.info("Start testClientServer...");
      ucpClientTest.testClientServer();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }
}
