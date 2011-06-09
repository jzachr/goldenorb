package org.goldenorb.types.message;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.goldenorb.Message;

public class RPCServer<M extends Message<W>, W extends Writable> implements RPCProtocol<M,W> {
  
  private int port;
  private Server server = null;
  
  private M message;
  
  public RPCServer(int port) {
    this.port = port;
    Configuration conf = new Configuration();
    try {
      server = RPC.getServer(this, "localhost", this.port, conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void start() {
    try {
      server.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
    
  @SuppressWarnings("unchecked")
  @Override
  public M sendAndReceiveMessage(M msg, String dst, W wrt) {
    M retVal = null;
    message = (M) msg;
    try {
      retVal = ((Class<M>) message.getClass()).newInstance();
      retVal.setDestinationVertex(dst);
      retVal.setMessageValue(wrt);
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return retVal;
  }
  
  public M getMessage() {
    return message;
  }
  
  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return versionID;
  }

  @Override
  public void stop() {
    server.stop();
  }
  
}
