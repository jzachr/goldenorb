 package org.goldenorb.types.message;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.goldenorb.Message;

public interface RPCProtocol<M extends Message<W>,W extends Writable> extends VersionedProtocol {
  public static final long versionID = 1L;
  
  public M sendAndReceiveMessage(M msg, String dst, W wrt);
  
  public M getMessage();
  
  public void start();
  
  public void stop();
}
