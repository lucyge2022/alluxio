package alluxio.worker.ucx;

import alluxio.AlluxioURI;
import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.DefaultPageMetaStore;
import alluxio.client.file.cache.LocalCacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.store.LocalPageStoreDir;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.worker.ucx.UcpServer;
import alluxio.worker.ucx.UcxDataReader;
import alluxio.conf.Configuration;
import alluxio.proto.dataserver.Protocol;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class UcxReadTest {
  private static Logger LOG = LoggerFactory.getLogger(UcxReadTest.class);
  
  public static UcpServer mServer;
//  public UcpContext mContext;
  public static LocalCacheManager mLocalCacheManager;
  private Random mRandom = new Random();
  private static InstancedConfiguration mConf = Configuration.copyGlobal();
  private static long sPageSize;
  @ClassRule
  public static TemporaryFolder mTemp = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.out.println("start beforeClass...");
//    PropertyConfigurator.configure("/root/github/alluxio/conf/lucy-log4j2.xml");
    Properties props = new Properties();
//    System.setProperty("myProperty", "lucy.log");
    props.setProperty(PropertyKey.LOGGER_TYPE.toString(), "Console");
//    props.setProperty(PropertyKey.CONF_DIR.toString(), "/root/github/alluxio/conf/");
//    props.setProperty(PropertyKey.LOGS_DIR.toString(), "/root/github/alluxio/logs/");

    mConf.set(PropertyKey.USER_CLIENT_CACHE_DIRS, mTemp.getRoot().getAbsolutePath());
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PageStoreType.LOCAL);
    sPageSize = mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE);
    CacheManagerOptions cacheManagerOptions = CacheManagerOptions.create(mConf);
    PageStoreOptions pageStoreOptions = PageStoreOptions.create(mConf).get(0);
    PageStore pageStore = PageStore.create(pageStoreOptions);
    PageStoreDir.clear(pageStoreOptions.getRootDir());
    CacheEvictor cacheEvictor = new FIFOCacheEvictor(cacheManagerOptions.getCacheEvictorOptions());
    PageStoreDir pageStoreDir = new LocalPageStoreDir(pageStoreOptions, pageStore, cacheEvictor);
    PageMetaStore pageMetaStore = new DefaultPageMetaStore(ImmutableList.of(pageStoreDir));
    mLocalCacheManager = LocalCacheManager.create(cacheManagerOptions, pageMetaStore);
    CommonUtils.waitFor("restore completed",
        () -> mLocalCacheManager.state() == CacheManager.State.READ_WRITE,
        WaitForOptions.defaults().setTimeoutMs(10000));
    mServer = UcpServer.getInstance(() -> {
      try {
        return new UcpServer(mLocalCacheManager);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
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

  public void prefill(String ufsPath, int numOfPages, SampleData sampleData) {
    Supplier<byte[]> externalDataSupplier = () -> {
      return sampleData.mData;
    };
    int totalPages = numOfPages;
    for (int i=0; i<totalPages; i++) {
      PageId pageId = new PageId(new AlluxioURI(ufsPath).hash(), i);
      mLocalCacheManager.cache(pageId, CacheContext.defaults(), externalDataSupplier);
    }
  }


  @Test
  public void testClientServer() throws Exception {
    String dummyUfsPath = "hdfs://localhost:9000/randomUfsPath";
    SampleData sampleData = new SampleData(generateRandomData(1024 * 1024));
    int numOfPages = 5;
    prefill(dummyUfsPath, numOfPages, sampleData);
    InetSocketAddress serverAddr = new InetSocketAddress(
//        "172.31.21.70", UcpServer.BIND_PORT);
        InetAddress.getLocalHost(), UcpServer.BIND_PORT);
//    System.out.println("Connecting to " + serverAddr.toString());
    Protocol.OpenUfsBlockOptions openUfsBlockOptions =
        Protocol.OpenUfsBlockOptions.newBuilder().setUfsPath(dummyUfsPath)
            .setOffsetInFile(0).setBlockSize(numOfPages * sPageSize)
            .setNoCache(true)
            .setMountId(0)
            .build();

    UcpContext ucpContext = new UcpContext(new UcpParams()
        .requestStreamFeature()
        .requestTagFeature()
        .requestRmaFeature()
        .requestWakeupFeature());
    UcpWorker worker = ucpContext.newWorker(new UcpWorkerParams()
        .requestWakeupRMA()
        .requestThreadSafety());

    int iteration = 1000;
    Protocol.ReadRequestRMA.Builder requestRMABuilder = Protocol.ReadRequestRMA.newBuilder()
        .setOpenUfsBlockOptions(openUfsBlockOptions);
    Protocol.ReadRequest.Builder requestBuilder = Protocol.ReadRequest.newBuilder()
        .setOpenUfsBlockOptions(openUfsBlockOptions);
    UcxDataReader reader = new UcxDataReader(serverAddr, worker, null, requestRMABuilder);
    Stopwatch stopwatch = Stopwatch.createStarted();
    for (int iter =0;iter<iteration;iter++) {
      System.out.println(String.format("iteration:%d", iter));
      reader.acquireServerConn();
      for (int i = 0; i < numOfPages; i++) {
        long position = i * sPageSize;// + mRandom.nextInt(pageSize);
        int length = (int)sPageSize;
        ByteBuffer buffer = ByteBuffer.allocateDirect(length);
        System.out.println(String.format("reading position:%s:length:%s", position, length));
        try {
          reader.read(position, buffer, length);
//        buffer.clear();
        System.out.println("buffer:" + buffer.toString());
        byte[] readContent = new byte[length];
        buffer.get(readContent);
        String readContentMd5 = "";
        try {
          MessageDigest md = MessageDigest.getInstance("MD5");
          md.update(readContent);
          readContentMd5 = Hex.encodeHexString(md.digest()).toLowerCase();
        } catch (NoSuchAlgorithmException e) {
          /* No actions. Continue with other hash method. */
        }
        System.out.println(String.format("readContentMd5:%s:sample data md5:%s",
            readContentMd5, sampleData.mMd5));
          Assert.assertEquals("md5 not equal", sampleData.mMd5, readContentMd5);
//        Assert.assertTrue(Arrays.equals(readContent, sampleData.mData));
        } catch (IOException e) {
          System.out.println("IOException on position:" + position + ":length:" + length);
          throw new RuntimeException(e);
        }
      }
    }
    long elapsedInMs = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    System.out.println(String.format("Total %d iteratons done, time taken in ms:%d",
        iteration, elapsedInMs));
  }
}
