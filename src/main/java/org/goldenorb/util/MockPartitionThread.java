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
package org.goldenorb.util;

import java.io.IOException;
import java.io.OutputStream;
import java.net.UnknownHostException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.Killable;
import org.goldenorb.Messages;
import org.goldenorb.OrbPartitionCommunicationProtocol;
import org.goldenorb.OrbPartitionManagerProtocol;
import org.goldenorb.PartitionProcess;
import org.goldenorb.Vertices;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.event.OrbCallback;
import org.goldenorb.event.OrbEvent;
import org.goldenorb.event.OrbExceptionEvent;
import org.goldenorb.io.input.RawSplit;
import org.goldenorb.jet.OrbPartitionMember;
import org.goldenorb.net.OrbDNS;
import org.goldenorb.zookeeper.LeaderGroup;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockPartitionThread extends OrbPartitionMember implements OrbConfigurable, PartitionProcess, Runnable, OrbPartitionCommunicationProtocol, OrbPartitionManagerProtocol {
  
  private final Logger logger = LoggerFactory.getLogger(MockPartitionThread.class);
  
  private Thread thread;
  private int processNum;
  private boolean standby = false;
  private String jobPath;
  private String jobInProgressPath;
  private ZooKeeper zk;
  private boolean waitingForJoin = true;
  private LeaderGroup<OrbPartitionMember> leaderGroup;
  private Server partitionsServer;
  private Server managerServer;
  private int partitionsPort;
  private int managerPort;
  private boolean run;
  private HeartbeatGenerator heartbeatGenerator;
  private String jobNumber;
  public boolean leader = false;
  
//  public MockPartitionThread() {
//    thread = new Thread(this);
//  }
  
  @Override
  public void launch(OutputStream outStream, OutputStream errStream) {
    thread = new Thread(this);
    partitionsPort = getOrbConf().getOrbBasePort() + processNum;
    managerPort = getOrbConf().getOrbBasePort() + processNum + 100;
    jobPath = "/GoldenOrb/" + getOrbConf().getOrbClusterName() + "/JobQueue/" + jobNumber;
    jobInProgressPath = "/GoldenOrb/" + getOrbConf().getOrbClusterName() + "/JobsInProgress/" + jobNumber;
    logger.info("jobPath: " + jobPath);
    logger.info("jobInProgressPath: " + jobInProgressPath);
    
    try {
      zk = ZookeeperUtils.connect(getOrbConf().getOrbZooKeeperQuorum());
    } catch (Exception e) {
      logger.error("Unable to establish a connection with ZooKeeper" + getOrbConf().getOrbZooKeeperQuorum(), e);
    }
    OrbConfiguration jobConf = null;
    try {
      jobConf = (OrbConfiguration) ZookeeperUtils.getNodeWritable(zk, jobPath, OrbConfiguration.class,
        getOrbConf());
    } catch (OrbZKFailure e) {
      logger.error("Unable to retrieve job from ZooKeeper: " + jobPath, e);
    }
    if (jobConf != null) {
      setOrbConf(jobConf);
    }

    thread.start();
  }

  @Override
  public void run() {
    try {
      setHostname(OrbDNS.getDefaultHost(getOrbConf()));
      setPort(this.managerPort);
      logger.debug("setting partition hostname " + getHostname());
    } catch (UnknownHostException e) {
      logger.error(e.getMessage());
    } 
    
    try {
      partitionsServer = RPC.getServer(this, getHostname(), this.partitionsPort, getOrbConf());
      partitionsServer.start();
      logger.info("starting RPC server on " + getHostname() + this.partitionsPort);
      managerServer = RPC.getServer(this, getHostname(), managerPort, getOrbConf());
      managerServer.start();
      logger.info("starting RPC server on " + getHostname() + this.managerPort);
    } catch (IOException e) {
      logger.error(e.getMessage());
    }

    try {
      initProxy(getOrbConf());
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
    
    leaderGroup = new LeaderGroup<OrbPartitionMember>(zk, new MockPartitionCallback(), jobInProgressPath, this, OrbPartitionMember.class);
    synchronized (this) {
      while(leaderGroup.getNumOfMembers() < (getOrbConf().getOrbRequestedPartitions() + getOrbConf().getOrbReservedPartitions())) {
        try {
          wait(10000);
        } catch (InterruptedException e) {
          logger.error(e.getMessage());
        }
      }
    }
    
    if(leaderGroup.isLeader()) {
      executeAsLeader();
    }
    else {
      executeAsSlave();
      
    }
//    synchronized (this) {
//      while(standby) {
//        try {
//          this.wait();
//        } catch (InterruptedException e) {
//          logger.error(e.getMessage());
//        }
//      }
//    }
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
    while (run) {
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
  
  private class MockPartitionCallback implements OrbCallback {
    
    @Override
    public void process(OrbEvent e) {
      int eventCode = e.getType();
      if (eventCode == OrbEvent.ORB_EXCEPTION) {
        ((OrbExceptionEvent) e).getException().printStackTrace();
      } else if (eventCode == OrbEvent.LEADERSHIP_CHANGE) {
        synchronized (MockPartitionThread.this) {
          if ((leaderGroup.isLeader() && !leader) || (!leaderGroup.isLeader() && leader)) {
            MockPartitionThread.this.notify();
          }
        }
      } else if (e.getType() == OrbEvent.NEW_MEMBER) {
        synchronized (MockPartitionThread.this) {
          if (waitingForJoin) {
            MockPartitionThread.this.notify();
          }
        }
      }
    }
  }

  /**
   * 
   */
  @Override
  public void kill() {
    try {
      thread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public OrbConfiguration getConf() {
    return getOrbConf();
  }
  
  @Override
  public void setConf(OrbConfiguration conf) {
    setOrbConf(conf);
  }
  
  @Override
  public int getProcessNum() {
    return processNum;
  }
  
  @Override
  public void setProcessNum(int processNum) {
    this.processNum = processNum;
  }
  
  @Override
  public boolean isRunning() {
    return thread.isAlive();
  }
  
  @Override
  public void setReserved(boolean reserved) {
    this.standby = reserved;
  }
  
  @Override
  public boolean isReserved() {
    return standby;
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return 0;
  }

  @Override
  public int stop() {
    return 0;
  }

  @Override
  public void sendVertices(Vertices vertices) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void sendMessages(Messages messages) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void becomeActive(int partitionID) {
    if(standby) {
      standby = false;
      synchronized (this) {
        notify();
      }
    }
  }

  @Override
  public void loadVerticesFromInputSplit(RawSplit rawsplit) {
    // TODO Auto-generated method stub
    
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
              ZookeeperUtils.existsUpdateNodeData(zk, jobInProgressPath + "/messages/hearbeat", new LongWritable(heartbeat++));
              logger.debug("creating heartbeat for: " + jobInProgressPath + "/messages/heartbeat = " + heartbeat);
            }
            catch( OrbZKFailure e) {
              logger.error(e.getMessage());
            }
          }
          catch(InterruptedException e) {
            logger.error(e.getMessage());
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

  @Override
  public void setJobNumber(String jobNumber) {
    this.jobNumber = jobNumber;    
  }

  @Override
  public String getJobNumber() {
    return jobNumber;
  }
}
