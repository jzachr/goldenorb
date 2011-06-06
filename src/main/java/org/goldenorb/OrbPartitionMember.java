package org.goldenorb;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.goldenorb.zookeeper.Member;


public class OrbPartitionMember implements Member{
	private String member;
	private int port;
	private String hostname;
	private int partitionID;
	
	public OrbPartitionMember(){};
	
	public OrbPartitionMember(String member, int port, String hostname, int partitionID, int coordinatorPort) {
		this.member = member;
		this.port = port;
		this.hostname = hostname;
		this.partitionID = partitionID;
		this.coordinatorPort = coordinatorPort;
	}
		
	public String getMember() {
		return member;
	}

	public void setMember(String member) {
		this.member = member;
	}

	public int getCoordinatorPort() {
		return coordinatorPort;
	}
	public void setCoordinatorPort(int coordinatorPort) {
		this.coordinatorPort = coordinatorPort;
	}
	private int coordinatorPort;
	
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getHostname() {
		return hostname;
	}
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public int getpartitionID() {
		return partitionID;
	}
	public void setpartitionID(int partitionID) {
		this.partitionID = partitionID;
	}

	public void readFields(DataInput in) throws IOException {
		member = in.readUTF();
		port = in.readInt();
		hostname = in.readUTF();
		partitionID = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(member);
		out.writeInt(port);
		out.writeUTF(hostname);
		out.writeInt(partitionID);
	}
}
