package alluxio.ucx;

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

import com.google.common.collect.ImmutableList;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Supplier;

public class UcxReadTest {

  public int serverPort = 1234;
  public String serverHost = "localhost";
  public UcpServer mServer;
//  public UcpContext mContext;
  public LocalCacheManager mLocalCacheManager;
  private Random mRandom = new Random();
  private InstancedConfiguration mConf = Configuration.copyGlobal();
  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();
  public UcpWorker mWorker;

  @Before
  public void before() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_DIRS, mTemp.getRoot().getAbsolutePath());
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PageStoreType.LOCAL);
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
    mServer = new UcpServer(mLocalCacheManager);
    mWorker = UcpServer.sGlobalContext.newWorker(new UcpWorkerParams());
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
  public void testClientServer() throws Exception {
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
    InetSocketAddress serverAddr = new InetSocketAddress(
        InetAddress.getLocalHost(), UcpServer.BIND_PORT);
    Protocol.OpenUfsBlockOptions openUfsBlockOptions =
        Protocol.OpenUfsBlockOptions.newBuilder().setUfsPath(dummyUfsPath)
            .setOffsetInFile(0).setBlockSize(totalLen)
            .setNoCache(true)
            .setMountId(0)
            .build();

    Protocol.ReadRequest.Builder requestBuilder = Protocol.ReadRequest.newBuilder()
        .setOpenUfsBlockOptions(openUfsBlockOptions);
    UcxDataReader reader = new UcxDataReader(serverAddr, mWorker, requestBuilder);
    reader.acquireServerConn();
    for (int i=0; i<totalPages; i++) {
      long position = i * pageSize + mRandom.nextInt(pageSize);
      int length = pageSize;
      ByteBuffer buffer = ByteBuffer.allocateDirect(length);
      System.out.println(String.format("reading position:%s:length:%s", position, length));
      try {
        reader.read(position, buffer, length);
        buffer.clear();
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
        Assert.assertTrue(Arrays.equals(readContent, sampleData.mData));
      } catch (IOException e) {
        System.out.println("IOException on position:" + position + ":length:" + length);
        throw new RuntimeException(e);
      }
    }
  }
}
