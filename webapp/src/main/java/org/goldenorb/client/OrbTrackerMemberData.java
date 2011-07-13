package org.goldenorb.client;

import java.io.Serializable;

public class OrbTrackerMemberData implements Serializable {
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
  
  /**
   * The name of the node in ZooKeeper.
   */
  private String name;
  
  /**
   * Default Constructor
   */
  public OrbTrackerMemberData() {}
  
  /**
   * Constructor
   * 
   * @param name
   *          - The String ZooKeeper uses to identify the node the data is gathered from
   * @param partitionCapacity
   *          - int
   * @param availablePartitions
   *          - int
   * @param reservedPartitions
   *          - int
   * @param inUsePartitions
   *          - int
   * @param hostname
   *          - String
   * @param leader
   *          -
   * @param port
   */
  public OrbTrackerMemberData(String name,
                              int partitionCapacity,
                              int availablePartitions,
                              int reservedPartitions,
                              int inUsePartitions,
                              String hostname,
                              boolean leader,
                              int port) {
    this.partitionCapacity = partitionCapacity;
    this.availablePartitions = availablePartitions;
    this.reservedPartitions = reservedPartitions;
    this.inUsePartitions = inUsePartitions;
    this.hostname = hostname;
    this.leader = leader;
    this.port = port;
    this.name = name;
  }
  
  public void setPartitionCapacity(int partitionCapacity) {
    this.partitionCapacity = partitionCapacity;
  }
  
  public int getPartitionCapacity() {
    return partitionCapacity;
  }
  
  public void setAvailablePartitions(int availablePartitions) {
    this.availablePartitions = availablePartitions;
  }
  
  public int getAvailablePartitions() {
    return availablePartitions;
  }
  
  public void setReservedPartitions(int reservedPartitions) {
    this.reservedPartitions = reservedPartitions;
  }
  
  public int getReservedPartitions() {
    return reservedPartitions;
  }
  
  public void setInUsePartitions(int inUsePartitions) {
    this.inUsePartitions = inUsePartitions;
  }
  
  public int getInUsePartitions() {
    return inUsePartitions;
  }
  
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }
  
  public String getHostname() {
    return hostname;
  }
  
  public void setLeaderStatus(boolean leader) {
    this.leader = leader;
  }
  
  public boolean getLeaderStatus() {
    return leader;
  }
  
  public void setPort(int port) {
    this.port = port;
  }
  
  public int getPort() {
    return port;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public String getName() {
    return name;
  }
}
