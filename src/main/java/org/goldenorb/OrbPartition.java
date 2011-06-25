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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.event.OrbCallback;
import org.goldenorb.event.OrbEvent;
import org.goldenorb.event.OrbExceptionEvent;
import org.goldenorb.jet.OrbPartitionMember;
import org.goldenorb.net.OrbDNS;
import org.goldenorb.zookeeper.LeaderGroup;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrbPartition extends OrbPartitionMember implements Runnable, OrbPartitionCommunicationProtocol,
    OrbPartitionManagerProtocol {
  
  private final static int PARTITION_JOIN_TIMEOUT = 60000;
  
  private static Logger LOG = LoggerFactory.getLogger(OrbPartition.class);
  
  /**
   * The unique identifier for this partition.
   */
  private int partitionID;
  private boolean leader = false;
  private String jobPath;
  private String jobInProgressPath;
  
  private ZooKeeper zk;
  
  private boolean standby;
  
  private boolean waitingForAllToJoin = true;
  
  private LeaderGroup<OrbPartitionMember> leaderGroup;
  
  private Server interpartitionCommunicationServer;
  private Server trackerTetherCommunicationServer;
  
  private int interpartitionCommunicationPort;
  private int trackerTetherCommunicationPort;
  
  private String hostname;
  
  private boolean runPartition;
  
  private HeartbeatGenerator heartbeatGenerator;
  
  public OrbPartition(String jobNumber, int partitionID, boolean standby, int partitionBasePort) {
    this.setOrbConf(new OrbConfiguration(true));
    this.standby = standby;
    interpartitionCommunicationPort = partitionBasePort;
    trackerTetherCommunicationPort = partitionBasePort + 100;
    jobPath = "/GoldenOrb/" + getOrbConf().getOrbClusterName() + "/JobQueue/" + jobNumber;
    jobInProgressPath = "/GoldenOrb/" + getOrbConf().getOrbClusterName() + "/JobsInProgress/" + jobNumber;
    this.partitionID = partitionID;
    try {
      ZookeeperUtils.connect(getOrbConf().getOrbZooKeeperQuorum());
    } catch (Exception e) {
      LOG.error("Unable to establish a connection with ZooKeeper" + getOrbConf().getOrbZooKeeperQuorum(), e);
      System.exit(-1);
    }
    OrbConfiguration jobConf = null;
    try {
      jobConf = (OrbConfiguration) ZookeeperUtils.getNodeWritable(zk, jobPath, OrbConfiguration.class,
        getOrbConf());
    } catch (OrbZKFailure e) {
      LOG.error("Unable to retrieve job from ZooKeeper: " + jobPath, e);
      System.exit(-1);
    }
    if (jobConf != null) {
      setOrbConf(jobConf);
    }
  }
  
  public static void main(String[] args) {
    if (args.length != 4) {
      LOG.error("OrbPartition cannot start unless it is passed both the partitionID and the jobNumber to the Jobs OrbConfiguration");
    }
    String jobNumber = args[0];
    int partitionID = Integer.parseInt(args[1]);
    boolean standby = Boolean.parseBoolean(args[2]);
    int partitionBasePort = Integer.parseInt(args[3]);
    new OrbPartition(jobNumber, partitionID, standby, partitionBasePort);
  }
  
  @Override
  public void run() {
    
    try {
      setHostname(OrbDNS.getDefaultHost(getOrbConf()));
    } catch (UnknownHostException e) {
      LOG.error("Unable to get hostname.", e);
      System.exit(-1);
    }
    
    try {
      interpartitionCommunicationServer = RPC.getServer(this, this.hostname,
        this.interpartitionCommunicationPort, getOrbConf());
      interpartitionCommunicationServer.start();
      LOG.info("Starting OrbPartition Interpartition Communication Server on: " + getHostname() + ":"
               + this.interpartitionCommunicationPort);
    } catch (IOException e) {
      LOG.error("Failed to start OrbPartition Interpartition Communication server!!", e);
      e.printStackTrace();
      System.exit(-1);
    }
    
    try {
      trackerTetherCommunicationServer = RPC.getServer(this, this.hostname,
        this.trackerTetherCommunicationPort, getOrbConf());
      trackerTetherCommunicationServer.start();
      LOG.info("Starting OrbPartition Tracker Tether Communication Server on: " + getHostname() + ":"
               + this.trackerTetherCommunicationPort);
    } catch (IOException e) {
      LOG.error("Failed to start Tracker Tether Communcation server!!", e);
      e.printStackTrace();
      System.exit(-1);
    }
    
    leaderGroup = new LeaderGroup<OrbPartitionMember>(zk, new OrbPartitionCallback(), jobInProgressPath,
        this, OrbPartitionMember.class);
    synchronized (this) {
      while (leaderGroup.getNumOfMembers() < (getOrbConf().getOrbRequestedPartitions() + getOrbConf()
          .getOrbReservedPartitions())) {
        try {
          wait(PARTITION_JOIN_TIMEOUT);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    
    // Loop waiting for it to become active. If it is already "active" then it should blow right through this.
    // i.e. if it is in this loop then it is waiting
    
    // synchronized(this){
    // while(standby){
    // LOG.info("OrbPartition " + partitionID + "is Standby partition.  Waiting for failure!");
    // try {
    // this.wait();
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // }
    // }
    // }
    
    synchronized (this) {
      while (standby) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
  
  private void executeAsSlave() {
    synchronized (this) {
      leader = false;
    }
    waitLoop();
  }
  
  private void executeAsLeader() {
    synchronized (this) {
      leader = true;
      heartbeatGenerator = new HeartbeatGenerator();
    }
    waitLoop();
  }
  
  private void waitLoop() {
    while (runPartition) {
      synchronized (this) {
        try {
          wait();
        } catch (InterruptedException e) {
          LOG.error(e.getMessage());
        }
      }
      if (leaderGroup.isLeader()) {
        executeAsLeader();
      } else {
        executeAsSlave();
      }
    }
  }
  
  private class OrbPartitionCallback implements OrbCallback {
    
    @Override
    public void process(OrbEvent e) {
      int eventCode = e.getType();
      if (eventCode == OrbEvent.ORB_EXCEPTION) {
        ((OrbExceptionEvent) e).getException().printStackTrace();
      } else if (eventCode == OrbEvent.LEADERSHIP_CHANGE) {
        synchronized (OrbPartition.this) {
          if ((leaderGroup.isLeader() && !leader) || (!leaderGroup.isLeader() && leader)) {
            OrbPartition.this.notify();
          }
        }
      } else if (e.getType() == OrbEvent.NEW_MEMBER) {
        synchronized (OrbPartition.this) {
          if (waitingForAllToJoin) {
            OrbPartition.this.notify();
          }
        }
      }
    }
    
  }
  
  public class OrbCommunicationInterface {
    public int superStep() {
      return 0;
    }
    
    public void voteToHalt(String vertexID) {}
    
    public void sendMessage(Message<? extends Writable> message) {}
  }
  
  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return 0L;
  }
  
  @Override
  public int stop() {
    // TODO Shutdown stuff
    return 0;
  }
  
  @Override
  public boolean isRunning() {
    // TODO what constitutes that it is no longer running?
    return true;
  }
  
  @Override
  public void sendVertices(Vertices vertices) {
    // TODO this should create a new handler to handle these vertices.
    
  }
  
  @Override
  public void sendMessages(Messages messages) {
    // TODO this should create a new handler to handle the inbound messages.
    
  }
  
  @Override
  public void becomeActive() {
    if (standby) {
      standby = false;
      synchronized (this) {
        notify();
      }
    }
  }
  
  private class HeartbeatGenerator implements Runnable, Killable {
    
    private boolean active = true;
    private Long heartbeat = 1L;
    
    @Override
    public void run() {
      while (active) {
        synchronized (this) {
          try {
            wait((getOrbConf().getJobHeartbeatTimeout() / 10));
            try {
              ZookeeperUtils.existsUpdateNodeData(zk, jobInProgressPath + "/messages/heartbeat", new LongWritable(heartbeat++));
              System.err.println("Creating heartbeat for: " + jobInProgressPath + "/messages/heartbeat" + " heartbeat is: " + heartbeat);
            } catch (OrbZKFailure e) {
              e.printStackTrace();
            }
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }

    @Override
    public void kill() {
      active = false;
    }

    @Override
    public void restart() {
      active = true;
    }
    
  }
}
