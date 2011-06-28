package org.goldenorb;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class VerticesRPCServer implements VerticesRPCProtocol {

  private Server server;
  public VerticesRPCServer(int port) {
    Configuration conf = new Configuration();
    try {
      server = RPC.getServer(this, "localhost", port, conf);
      server.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public Vertices sendAndReceive(Vertices vs) {
    Vertices ret = null;
    try {
      ret = ((Class<Vertices>) vs.getClass()).newInstance();
      for (Vertex<?,?,?> v : vs.getArrayList()) {
        ret.add(v);
      }
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return ret;
  }
}
