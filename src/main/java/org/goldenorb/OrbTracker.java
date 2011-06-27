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
 */
package org.goldenorb;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.event.OrbCallback;
import org.goldenorb.event.OrbEvent;
import org.goldenorb.event.OrbExceptionEvent;
import org.goldenorb.jet.OrbTrackerMember;
import org.goldenorb.jet.PartitionRequest;
import org.goldenorb.jet.PartitionRequestResponse;
import org.goldenorb.net.OrbDNS;
import org.goldenorb.util.ResourceAllocator;
import org.goldenorb.zookeeper.LeaderGroup;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OrbTracker extends {@link OrbTrackerMember}, which allows them to run as either 
 * leaders or slaves. The leader OrbTracker is responsible for creating the LeaderGroup and coordinating 
 * with other OrbTrackers via ZooKeeper and Hadoop RPC. OrbTracker also starts the {@link JobManager} 
 * and {@link OrbPartitionManager}.
 *
 */
public class OrbTracker extends OrbTrackerMember implements Runnable, OrbConfigurable {
  
  public static final String ZK_BASE_PATH = "/GoldenOrb";
  
  private final Logger logger = LoggerFactory.getLogger(OrbTracker.class);
  
//  private OrbConfiguration orbConf;
  private ZooKeeper zk;
  private LeaderGroup<OrbTrackerMember> leaderGroup;
  private Server server = null;
  private boolean leader = false;
  private JobManager<OrbTrackerMember> jobManager;
  private OrbCallback orbCallback;
  private boolean runTracker = true;
  private ResourceAllocator<OrbTrackerMember> resourceAllocator;
  private OrbPartitionManager<OrbPartitionProcess> partitionManager;
  
/**
 * 
 * @param  String[] args
 */
  public static void main(String[] args) {
    new Thread(new OrbTracker(new OrbConfiguration(true))).start();
  }
  
/**
 * Constructor
 *
 * @param  OrbConfiguration orbConf
 */
  public OrbTracker(OrbConfiguration orbConf) {
    setOrbConf(orbConf);
  }
  
/**
 * 
 */
  public void run() {
    // get hostname
    try {
      setHostname(OrbDNS.getDefaultHost(getOrbConf()));
      setPort(getOrbConf().getOrbTrackerPort());
      logger.info("Starting OrbTracker on: " + getHostname() + getPort());
    } catch (UnknownHostException e) {
      logger.error("Unable to get hostname.", e);
      System.exit(-1);
    }
    
    // startServer
    try {
      logger.info("starting RPC server on " + getHostname() + ":" + getPort());
      server = RPC.getServer(this, getHostname(), getPort(), getOrbConf());
      server.start();
      
      logger.info("starting OrbPartitionManager");
      // change from MockPartitionThread to OrbPartitionProcess
      partitionManager = new OrbPartitionManager<OrbPartitionProcess>(getOrbConf(), OrbPartitionProcess.class);
    } catch (IOException e) {
      logger.error("Unable to get hostname.", e);
      System.exit(-1);
    }
    
    // connect to zookeeper
    try {
      establishZookeeperConnection();
    } catch (Exception e) {
      logger.error("Failed to connect to Zookeeper", e);
      System.exit(-1);
    }
    
    // establish the zookeeper tree and join the cluster
    try {
      establishZookeeperTree();
    } catch (OrbZKFailure e) {
      logger.error("Major Zookeeper Error: ", e);
      System.exit(-1);
    }
    
    if (leaderGroup.isLeader()) {
      executeAsLeader();
    } else {
      executeAsSlave();
    }
  }
  
/**
 * 
 */
  private void executeAsSlave() {
    synchronized (this) {
      leader = false;
      if (jobManager != null) {
        jobManager.shutdown();
      }
    }
    waitLoop();
  }
  
/**
 * 
 */
  private void executeAsLeader() {
    synchronized (this) {
      resourceAllocator = new ResourceAllocator<OrbTrackerMember>(getOrbConf(), leaderGroup.getMembers());
      leader = true;
      orbCallback = new OrbTrackerCallback();
      jobManager = new JobManager<OrbTrackerMember>(orbCallback, getOrbConf(), zk, resourceAllocator,
          leaderGroup.getMembers());
    }
    waitLoop();
  }
  
/**
 * 
 */
  private void waitLoop() {
    while (runTracker) {
      synchronized (this) {
        try {
          wait();
        } catch (InterruptedException e) {
          logger.error(e.getMessage());
        }
      }
      if (leaderGroup.isLeader()) {
        executeAsLeader();
      } else {
        executeAsSlave();
      }
    }
  }
  
/**
 * 
 */
  private void establishZookeeperTree() throws OrbZKFailure {
    ZookeeperUtils.notExistCreateNode(zk, ZK_BASE_PATH);
    ZookeeperUtils.notExistCreateNode(zk, ZK_BASE_PATH + "/" + getOrbConf().getOrbClusterName());
    ZookeeperUtils.notExistCreateNode(zk, ZK_BASE_PATH + "/" + getOrbConf().getOrbClusterName() + "/OrbTrackers");
    
    if (ZookeeperUtils.nodeExists(zk, ZK_BASE_PATH + "/" + getOrbConf().getOrbClusterName() + "/OrbTrackers/"
                                      + getHostname())) {
      logger.info("Already have an OrbTracker on " + getHostname() + "(Exiting)");
      System.exit(-1);
    } else {
      ZookeeperUtils.tryToCreateNode(zk, ZK_BASE_PATH + "/" + getOrbConf().getOrbClusterName() + "/OrbTrackers/"
                                         + getHostname(), CreateMode.EPHEMERAL);
    }
    this.setAvailablePartitions(getOrbConf().getNumberOfPartitionsPerMachine());
    this.setInUsePartitions(0);
    this.setReservedPartitions(0);
    this.setLeader(false);
    this.setPartitionCapacity(getOrbConf().getNumberOfPartitionsPerMachine());
    leaderGroup = new LeaderGroup<OrbTrackerMember>(zk, new OrbTrackerCallback(),
        ZK_BASE_PATH + "/" + getOrbConf().getOrbClusterName() + "/OrbTrackerLeaderGroup", this,
        OrbTrackerMember.class);
  }
  
  public class OrbTrackerCallback implements OrbCallback {
/**
 * 
 * @param  OrbEvent e
 */
    @Override
    public void process(OrbEvent e) {
      int eventCode = e.getType();
      if (eventCode == OrbEvent.ORB_EXCEPTION) {
        ((OrbExceptionEvent) e).getException().printStackTrace();
      } else if (eventCode == OrbEvent.LEADERSHIP_CHANGE) {
        synchronized (OrbTracker.this) {
          if ((leaderGroup.isLeader() && !leader) || (!leaderGroup.isLeader() && leader)) {
            OrbTracker.this.notify();
          }
        }
      }
    }
  }
  
/**
 * 
 */
  public void leave() {
    runTracker = false;
    leaderGroup.leave();
    if (jobManager != null) {
      jobManager.shutdown();
    }
  }
  
/**
 * 
 */
  private void establishZookeeperConnection() throws IOException, InterruptedException {
    zk = ZookeeperUtils.connect(getOrbConf().getOrbZooKeeperQuorum());
  }
  
/**
 * 
 * @param  PartitionRequest request
 * @returns PartitionRequestResponse
 */
  @Override
  public PartitionRequestResponse requestPartitions(PartitionRequest request) {
    logger.info("requestPartitions");
    PartitionRequestResponse response = null;
    try {
      /* response = */partitionManager.launchPartitions(request.getActivePartitions(),
        request.getReservedPartitions(), request.getBasePartitionID(), request.getJobID());
    } catch (InstantiationException e) {
      logger.error(e.getMessage());
    } catch (IllegalAccessException e) {
      logger.error(e.getMessage());
    }
    
    return response;
  }
  
  @Override
  public void killJob(String jobNumber){
    partitionManager.kill(jobNumber);
  }
}
