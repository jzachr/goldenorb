package org.goldenorb;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.goldenorb.io.input.RawSplit;

public interface OrbPartitionCommunicationProtocol extends VersionedProtocol {
	public static final long versionID = 0L;
	
	public void sendVertices(Vertices vertices);
	
	public void sendMessages(Messages messages);
	
	public void becomeActive();
	
	public void loadVerticesFromInputSplit(RawSplit rawsplit);
}
