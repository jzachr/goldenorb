package org.goldenorb;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface OrbPartitionCommunicationProtocol extends VersionedProtocol {
	public static final long versionID = 0L;
	
	public void sendVertices(Vertices vertices);
	
	public void sendMessages(Messages messages);
}
