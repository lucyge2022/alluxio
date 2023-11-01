package alluxio.worker.ucx;

import alluxio.concurrent.jsr.CompletableFuture;

import org.junit.BeforeClass;
import org.junit.Test;
import org.openucx.jucx.ucp.UcpConnectionRequest;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerConnectionHandler;
import org.openucx.jucx.ucp.UcpListenerParams;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class UcxConnectionTest {
  public static UcpContext sGlobalContext;

  @BeforeClass
  public static void initContext() {
    System.out.println("start initContext...");
    sGlobalContext = new UcpContext(new UcpParams()
        .requestStreamFeature()
        .requestTagFeature()
        .requestWakeupFeature());
  }


  @Test
  public void testEstablishConnection() throws Exception {
    InetAddress localAddr = InetAddress.getLocalHost();
    int serverPort = 1234;
    UcpWorker serverWorker = sGlobalContext.newWorker(new UcpWorkerParams().requestThreadSafety());
    CompletableFuture<UcpConnectionRequest> incomingConn = new CompletableFuture<>();
    UcpListenerParams listenerParams = new UcpListenerParams()
        .setConnectionHandler(new UcpListenerConnectionHandler() {
          @Override
          public void onConnectionRequest(UcpConnectionRequest connectionRequest) {
            incomingConn.complete(connectionRequest);
//            mConnectionRequests.offer(connectionRequest);
          }
        });
    InetSocketAddress remoteAddr = new InetSocketAddress(localAddr, serverPort);
    UcpListener ucpListener = serverWorker.newListener(
        listenerParams.setSockAddr(remoteAddr));
    Thread serverThread = new Thread(() -> {
      try {
        while (serverWorker.progress() == 0) {
          System.out.println("Nothing to progress, waiting on events..");
          serverWorker.waitForEvents();
        }
        UcpConnectionRequest incomeConnReq = incomingConn.get();
        if (incomeConnReq != null) {
          UcpEndpoint bootstrapEp = serverWorker.newEndpoint(new UcpEndpointParams()
              .setPeerErrorHandlingMode()
              .setConnectionRequest(incomeConnReq));
          UcxConnection ucxConnection = UcxConnection.acceptIncomingConnection(
              bootstrapEp, serverWorker, incomeConnReq.getClientAddress());
          System.out.println("Conn established from server:" + ucxConnection.toString());
        }
      } catch (Exception e) {
        System.out.println("Exception in server thread.");
        e.printStackTrace();
      }
    });
    serverThread.start();

    UcpWorker clientWorker = sGlobalContext.newWorker(new UcpWorkerParams().requestThreadSafety());
    // client init conn
    System.out.println("Starting init new conn...");
    UcxConnection connToServer = UcxConnection.initNewConnection(remoteAddr, clientWorker);
    System.out.println("Conn established to server:" + connToServer.toString());
    serverThread.join();
  }

}
