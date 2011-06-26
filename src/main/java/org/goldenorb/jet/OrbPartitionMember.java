/**
 * Licensed to Ravel, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Ravel, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package org.goldenorb.jet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;

/*
 * Start of non-generated import declaration code -- any code written outside of this block will be
 * removed in subsequent code generations.
 */
import org.goldenorb.Messages;
import org.goldenorb.OrbPartitionCommunicationProtocol;
import org.goldenorb.Vertices;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.io.input.RawSplit;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/* End of non-generated import declaraction code */

/**
 * This class is the proxy object for an OrbPartition into the LeaderGroup
 */
public class OrbPartitionMember implements org.goldenorb.zookeeper.Member,
    org.goldenorb.OrbPartitionCommunicationProtocol, org.goldenorb.conf.OrbConfigurable {
  
  /**
   * the id assigned to this partition
   */
  private int partitionID;
  
  /**
   * the total number vertices assigned to this partition
   */
  private int numberOfVertices;
  
  /**
   * the current superstep that the OrbPartition is on
   */
  private int superStep;
  
  /**
   * the total number of messages sent so far
   */
  private int messagesSent;
  
  /**
   * the percent complete for this superstep
   */
  private float percentComplete;
  
  /**
   * the host name of the machine running this OrbTracker
   */
  private String hostname;
  
  /**
   * whether this member is the leader
   */
  private boolean leader;
  
  /**
   * the port number the OrbTracker provides RPC on
   */
  private int port;
  
  /*
   * Start of non-generated variable declaration code -- any code written outside of this block will be
   * removed in subsequent code generations.
   */
  private OrbPartitionCommunicationProtocol client;
  private OrbConfiguration orbConf;
  
  /* End of non-generated variable declaraction code */

  /**
   * 
   */
  public OrbPartitionMember() {}
  
  /*
   * Start of non-generated method code -- any code written outside of this block will be removed in
   * subsequent code generations.
   */
/**
 * Return the protocolVersion
 */
  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return this.versionID;
  }
  
/**
 * Set the orbConf
 * @param  OrbConfiguration orbConf
 */
  @Override
  public void setOrbConf(OrbConfiguration orbConf) {
    this.orbConf = orbConf;
  }
  
/**
 * Return the orbConf
 */
  @Override
  public OrbConfiguration getOrbConf() {
    return orbConf;
  }
  
/**
 * 
 * @param  Vertices vertices
 */
  @Override
  public void sendVertices(Vertices vertices) {
    client.sendVertices(vertices);
  }
  
/**
 * 
 * @param  Messages messages
 */
  @Override
  public void sendMessages(Messages messages) {
    client.sendMessages(messages);
  }
  
/**
 * 
 */
  public void initProxy() throws IOException {
    initProxy(this.orbConf);
  }
  
/**
 * 
 * @param  int partitionID
 */
  @Override
  public void becomeActive(int partitionID) {
    client.becomeActive(partitionID);
  }
  
/**
 * 
 * @param  RawSplit rawsplit
 */
  @Override
  public void loadVerticesFromInputSplit(RawSplit rawsplit) {
    client.loadVerticesFromInputSplit(rawsplit);
  }
  
/**
 * 
 * @param  OrbConfiguration orbConf
 */
  public void initProxy(OrbConfiguration orbConf) throws IOException {
    if(client == null){
      InetSocketAddress addr = new InetSocketAddress(hostname, port);
      client = (OrbPartitionCommunicationProtocol) RPC.waitForProxy(OrbPartitionCommunicationProtocol.class,
        OrbPartitionCommunicationProtocol.versionID, addr, orbConf);
    }
  }
  
/**
 * 
 * @param  Object rhs
 * @returns boolean
 */
  public boolean equals(Object rhs) {
    return hostname.equals(((OrbPartitionMember) rhs).getHostname()) && (port == (((OrbPartitionMember) rhs).getPort()));
  }
  
  /* End of non-generated method code */

  /**
   * gets the id assigned to this partition
   * 
   * @return
   */
  public int getPartitionID() {
    return partitionID;
  }
  
  /**
   * sets the id assigned to this partition
   * 
   * @param partitionID
   */
  public void setPartitionID(int partitionID) {
    this.partitionID = partitionID;
  }
  
  /**
   * gets the total number vertices assigned to this partition
   * 
   * @return
   */
  public int getNumberOfVertices() {
    return numberOfVertices;
  }
  
  /**
   * sets the total number vertices assigned to this partition
   * 
   * @param numberOfVertices
   */
  public void setNumberOfVertices(int numberOfVertices) {
    this.numberOfVertices = numberOfVertices;
  }
  
  /**
   * gets the current superstep that the OrbPartition is on
   * 
   * @return
   */
  public int getSuperStep() {
    return superStep;
  }
  
  /**
   * sets the current superstep that the OrbPartition is on
   * 
   * @param superStep
   */
  public void setSuperStep(int superStep) {
    this.superStep = superStep;
  }
  
  /**
   * gets the total number of messages sent so far
   * 
   * @return
   */
  public int getMessagesSent() {
    return messagesSent;
  }
  
  /**
   * sets the total number of messages sent so far
   * 
   * @param messagesSent
   */
  public void setMessagesSent(int messagesSent) {
    this.messagesSent = messagesSent;
  }
  
  /**
   * gets the percent complete for this superstep
   * 
   * @return
   */
  public float getPercentComplete() {
    return percentComplete;
  }
  
  /**
   * sets the percent complete for this superstep
   * 
   * @param percentComplete
   */
  public void setPercentComplete(float percentComplete) {
    this.percentComplete = percentComplete;
  }
  
  /**
   * gets the host name of the machine running this OrbTracker
   * 
   * @return
   */
  public String getHostname() {
    return hostname;
  }
  
  /**
   * sets the host name of the machine running this OrbTracker
   * 
   * @param hostname
   */
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }
  
  /**
   * gets whether this member is the leader
   * 
   * @return
   */
  public boolean isLeader() {
    return leader;
  }
  
  /**
   * sets whether this member is the leader
   * 
   * @param leader
   */
  public void setLeader(boolean leader) {
    this.leader = leader;
  }
  
  /**
   * gets the port number the OrbTracker provides RPC on
   * 
   * @return
   */
  public int getPort() {
    return port;
  }
  
  /**
   * sets the port number the OrbTracker provides RPC on
   * 
   * @param port
   */
  public void setPort(int port) {
    this.port = port;
  }
  
  // /////////////////////////////////////
  // Writable
  // /////////////////////////////////////
/**
 * 
 * @param  DataInput in
 */
  public void readFields(DataInput in) throws IOException {
    partitionID = in.readInt();
    numberOfVertices = in.readInt();
    superStep = in.readInt();
    messagesSent = in.readInt();
    percentComplete = in.readFloat();
    hostname = Text.readString(in);
    leader = in.readBoolean();
    port = in.readInt();
  }
  
/**
 * 
 * @param  DataOutput out
 */
  public void write(DataOutput out) throws IOException {
    out.writeInt(partitionID);
    out.writeInt(numberOfVertices);
    out.writeInt(superStep);
    out.writeInt(messagesSent);
    out.writeFloat(percentComplete);
    Text.writeString(out, hostname);
    out.writeBoolean(leader);
    out.writeInt(port);
  }
  
}
