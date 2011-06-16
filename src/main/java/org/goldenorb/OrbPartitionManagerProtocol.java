package org.goldenorb;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface OrbPartitionManagerProtocol extends VersionedProtocol {
  public static final long versionID = 0L;
  public int stop();
  public boolean isRunning();
}
