package alluxio.membership;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.dora.PagedDoraWorker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class StaticMembershipManager implements MembershipManager {
  List<WorkerInfo> mMembers;

  private final AlluxioConfiguration mConf;
  public StaticMembershipManager(AlluxioConfiguration conf) throws IOException {
    mConf = conf;
    String workerListFile = conf.getString(PropertyKey.WORKER_MEMBER_STATIC_CONFIG_FILE);
    // user conf/workers, use default port
    mMembers = parseWorkerAddresses(workerListFile, mConf);
  }

  /**
   *
   * @param configFile
   * @param conf
   * @return
   * @throws IOException
   */
  public static List<WorkerInfo> parseWorkerAddresses(
      String configFile, AlluxioConfiguration conf) throws IOException {
    List<WorkerNetAddress> workerAddrs = new ArrayList<>();
    File file = new File(configFile);
    if (!file.exists()) {
      throw new FileNotFoundException("Not found for static worker config file:" + configFile);
    }
    Scanner scanner = new Scanner(new File("filename"));
    while (scanner.hasNextLine()) {
      String addr = scanner.nextLine();
      addr.trim();
      WorkerNetAddress workerNetAddress = new WorkerNetAddress()
          .setContainerHost(Configuration.global()
              .getOrDefault(PropertyKey.WORKER_CONTAINER_HOSTNAME, ""))
          .setRpcPort(conf.getInt(PropertyKey.WORKER_RPC_PORT))
          .setWebPort(conf.getInt(PropertyKey.WORKER_WEB_PORT));
      workerAddrs.add(workerNetAddress);
    }
    return workerAddrs.stream()
        .map(w -> new WorkerInfo().setAddress(w)).collect(Collectors.toList());
  }

  @Override
  public void join(WorkerInfo worker) throws IOException {
    // NO OP
  }

  @Override
  public List<WorkerInfo> getAllMembers() {
    return mMembers;
  }

  @Override
  public List<WorkerInfo> getLiveMembers() {
    // No op for static type membership manager
    return mMembers;
  }

  @Override
  public List<WorkerInfo> getFailedMembers() {
    // No op for static type membership manager
    return Collections.emptyList();
  }

  @Override
  public String showAllMembers() {
    String printFormat = "%s\t%s\t%s\n";
    StringBuilder sb = new StringBuilder(
        String.format(printFormat, "WorkerId", "Address", "Status"));
    for (WorkerInfo worker : getAllMembers()) {
      String entryLine = String.format(printFormat,
          CommonUtils.hashAsStr(worker.getAddress().dumpMainInfo()),
          worker.getAddress().getHost() + ":" + worker.getAddress().getRpcPort(),
          "N/A");
      sb.append(entryLine);
    }
    return sb.toString();
  }

  @Override
  public void decommission(WorkerInfo worker) {
    mMembers.remove(worker);
  }

  @Override
  public void close() throws Exception {
    // Nothing to close
  }
}
