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
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

public class TestUcxConnection {
  public static UcpContext sGlobalContext;

  @BeforeClass
  public void initContext() {
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
        e.printStackTrace();
      }
    });
    serverThread.start();

    UcpWorker clientWorker = sGlobalContext.newWorker(new UcpWorkerParams().requestThreadSafety());
    // client init conn
    UcxConnection connToServer = UcxConnection.initNewConnection(remoteAddr, clientWorker);
    System.out.println("Conn established to server:" + connToServer.toString());
    serverThread.join();
  }

}
