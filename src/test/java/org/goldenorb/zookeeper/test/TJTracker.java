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
package org.goldenorb.zookeeper.test;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.JobManager;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.event.OrbCallback;
import org.goldenorb.event.OrbEvent;
import org.goldenorb.event.OrbExceptionEvent;
import org.goldenorb.jet.OrbTrackerMember;
import org.goldenorb.util.ResourceAllocator;
import org.goldenorb.zookeeper.LeaderGroup;

public class TJTracker implements Runnable, OrbConfigurable {
  
  private ZooKeeper zk;
  private CountDownLatch joinLeaderGroup;
  private CountDownLatch exit;
  private OrbConfiguration orbConf;
  private TMember member;
  private LeaderGroup<TMember> leaderGroup;
  private String basePath;
  private JobManager jobManager;
  private OrbCallback orbCallback;
  private boolean runTracker = true;
  private boolean leader = false;
  private ResourceAllocator allocator;
  
/**
 * Constructor
 *
 * @param  ZooKeeper zk
 * @param  CountDownLatch joinLeaderGroup
 * @param  CountDownLatch exit
 * @param  OrbConfiguration orbConf
 * @param  int data
 * @param  String basePath
 */
  public TJTracker(ZooKeeper zk,
                   CountDownLatch joinLeaderGroup,
                   CountDownLatch exit,
                   OrbConfiguration orbConf,
                   int data,
                   String basePath) {
    this.joinLeaderGroup = joinLeaderGroup;
    this.exit = exit;
    this.zk = zk;
    this.orbConf = orbConf;
    this.basePath = basePath;
    member = new TMember();
    member.setData(data);
  }
  
/**
 * 
 */
  @Override
  public void run() {
    orbCallback = new OrbTJTrackerCallback();
    leaderGroup = new LeaderGroup<TMember>(zk, orbCallback, "/GoldenOrb/" + orbConf.getOrbClusterName()
                                                            + "/OrbTrackerLeaderGroup", member, TMember.class);
    joinLeaderGroup.countDown();
    if (leaderGroup.isLeader()) {
      leader();
    } else {
      slave();
    }
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
 */
  private void leader() {
    synchronized (this) {
      leader = true;
      allocator = new ResourceAllocator(orbConf, leaderGroup.getMembers());
      jobManager = new JobManager(orbCallback, orbConf, zk, allocator, leaderGroup.getMembers());
    }
    waitLoop();
  }
  
/**
 * 
 */
  private void slave() {
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
  private void waitLoop() {
    while (runTracker) {
      synchronized (this) {
        try {
          wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      if (leaderGroup.isLeader()) {
        leader();
      } else {
        slave();
      }
    }
  }
  
  private class OrbTJTrackerCallback implements OrbCallback {
    
/**
 * 
 * @param  OrbEvent e
 */
    @Override
    public void process(OrbEvent e) {
      if (e.getType() == OrbEvent.ORB_EXCEPTION) {
        ((OrbExceptionEvent) e).getException().printStackTrace();
      } else {
        if (e.getType() == OrbEvent.LEADERSHIP_CHANGE) {
          synchronized (TJTracker.this) {
            if ((leaderGroup.isLeader() && !leader) || (!leaderGroup.isLeader() && leader)) {
              TJTracker.this.notify();
            }
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
    exit.countDown();
  }

  
  
/**
 * Return the eader
 */
  public boolean isLeader() {
    return leaderGroup.isLeader();
  }
  
}
