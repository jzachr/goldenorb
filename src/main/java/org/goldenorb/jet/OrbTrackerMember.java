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
import org.goldenorb.OrbTrackerCommunicationProtocol;
import org.goldenorb.conf.OrbConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.ipc.RPC;
import java.net.InetSocketAddress;

/* End of non-generated import declaraction code */

/**
 * This class is the proxy object for an OrbTracker into the LeaderGroup
 */
public class OrbTrackerMember implements org.goldenorb.zookeeper.Member, org.goldenorb.OrbTrackerCommunicationProtocol, org.goldenorb.conf.OrbConfigurable {
  
  /**
   * the total number of partitions that this OrbTracker can handle
   */
  private int partitionCapacity;
  
  /**
   * the total number of partitions that the OrbTracker currently has available
   */
  private int availablePartitions;
  
  /**
   * the total number of partitions that are reserved for failures on this OrbTracker
   */
  private int reservedPartitions;
  
  /**
   * the total number of partitions that are currently in on this OrbTracker
   */
  private int inUsePartitions;
  
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

  private OrbTrackerCommunicationProtocol client;
  private OrbConfiguration orbConf;
  private final Logger logger = LoggerFactory.getLogger(OrbTrackerMember.class);
  
  /* End of non-generated variable declaraction code */

  /**
   * 
   */
  public OrbTrackerMember() {}
  
  /*
   * Start of non-generated method code -- any code written outside of this block will be removed in
   * subsequent code generations.
   */

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
 * @param  Object rhs
 * @returns boolean
 */
  public boolean equals(Object rhs) {
    return hostname.equals(((OrbTrackerMember) rhs).getHostname());
  }
  
/**
 * 
 */
  public void initProxy()  {
    initProxy(this.orbConf);
  }
  
/**
 * 
 * @param  OrbConfiguration orbConf
 * @throws IOException 
 */
  public void initProxy(OrbConfiguration orbConf)  {
    InetSocketAddress addr = new InetSocketAddress(hostname, port);
    logger.info("Trying to initialize: " + hostname + ":" + port);
    try {
      client = (OrbTrackerCommunicationProtocol) RPC.getProxy(OrbTrackerCommunicationProtocol.class,
      OrbTrackerCommunicationProtocol.versionID, addr, orbConf);
    logger.info("Trying to initialize: " + hostname + ":" + port);

    } catch (IOException e){
      e.printStackTrace();
    }
  }
  
/**
 * Return the protocolVersion
 */
  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return versionID;
  }
  
/**
 * 
 * @param  PartitionRequest partitionRequest
 * @returns PartitionRequestResponse
 */
  @Override
  public PartitionRequestResponse requestPartitions(PartitionRequest partitionRequest) {
    logger.info("requestPartitions");
    return client.requestPartitions(partitionRequest);
  }
  
  
  @Override
  public void killJob(String jobNumber){
    client.killJob(jobNumber);
  }

  /* End of non-generated method code */
  
  /**
   * gets the total number of partitions that this OrbTracker can handle
   * @return
   */
  public int getPartitionCapacity() {
    return partitionCapacity;
  }
  
  /**
   * sets the total number of partitions that this OrbTracker can handle
   * @param partitionCapacity
   */
  public void setPartitionCapacity(int partitionCapacity) {
    this.partitionCapacity = partitionCapacity;
  }
  
  /**
   * gets the total number of partitions that the OrbTracker currently has available
   * @return
   */
  public int getAvailablePartitions() {
    return availablePartitions;
  }
  
  /**
   * sets the total number of partitions that the OrbTracker currently has available
   * @param availablePartitions
   */
  public void setAvailablePartitions(int availablePartitions) {
    this.availablePartitions = availablePartitions;
  }
  
  /**
   * gets the total number of partitions that are reserved for failures on this OrbTracker
   * @return
   */
  public int getReservedPartitions() {
    return reservedPartitions;
  }
  
  /**
   * sets the total number of partitions that are reserved for failures on this OrbTracker
   * @param reservedPartitions
   */
  public void setReservedPartitions(int reservedPartitions) {
    this.reservedPartitions = reservedPartitions;
  }
  
  /**
   * gets the total number of partitions that are currently in on this OrbTracker
   * @return
   */
  public int getInUsePartitions() {
    return inUsePartitions;
  }
  
  /**
   * sets the total number of partitions that are currently in on this OrbTracker
   * @param inUsePartitions
   */
  public void setInUsePartitions(int inUsePartitions) {
    this.inUsePartitions = inUsePartitions;
  }
  
  /**
   * gets the host name of the machine running this OrbTracker
   * @return
   */
  public String getHostname() {
    return hostname;
  }
  
  /**
   * sets the host name of the machine running this OrbTracker
   * @param hostname
   */
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }
  
  /**
   * gets whether this member is the leader
   * @return
   */
  public boolean isLeader() {
    return leader;
  }
  
  /**
   * sets whether this member is the leader
   * @param leader
   */
  public void setLeader(boolean leader) {
    this.leader = leader;
  }
  
  /**
   * gets the port number the OrbTracker provides RPC on
   * @return
   */
  public int getPort() {
    return port;
  }
  
  /**
   * sets the port number the OrbTracker provides RPC on
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
    partitionCapacity = in.readInt();
    availablePartitions = in.readInt();
    reservedPartitions = in.readInt();
    inUsePartitions = in.readInt();
    hostname = Text.readString(in);
    leader = in.readBoolean();
    port = in.readInt();
  }
  
/**
 * 
 * @param  DataOutput out
 */
  public void write(DataOutput out) throws IOException {
    out.writeInt(partitionCapacity);
    out.writeInt(availablePartitions);
    out.writeInt(reservedPartitions);
    out.writeInt(inUsePartitions);
    Text.writeString(out, hostname);
    out.writeBoolean(leader);
    out.writeInt(port);
  }
  
}
