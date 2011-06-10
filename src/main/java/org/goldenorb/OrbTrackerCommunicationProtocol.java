package org.goldenorb;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.goldenorb.jet.PartitionRequest;
import org.goldenorb.jet.PartitionRequestResponse;

public interface OrbTrackerCommunicationProtocol extends VersionedProtocol {
  
  long versionID = 1L;
  public PartitionRequestResponse requestPartitions(PartitionRequest partitionRequest);
}
