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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.event.LeadershipChangeEvent;
import org.goldenorb.event.LostMemberEvent;
import org.goldenorb.event.MemberDataChangeEvent;
import org.goldenorb.event.NewMemberEvent;
import org.goldenorb.event.OrbCallback;
import org.goldenorb.event.OrbEvent;
import org.goldenorb.event.OrbExceptionEvent;
import org.goldenorb.zookeeper.LeaderGroup;
import org.goldenorb.zookeeper.OrbZKFailure;

public class TTracker implements Runnable {
  
/**
 * Constructor
 *
 * @param  ZooKeeper zk
 * @param  int data
 * @param  String basePath
 * @param  CountDownLatch startCdl
 * @param  CountDownLatch leaderChangeCdl
 * @param  CountDownLatch leaveCdl
 * @param  CountDownLatch dataChangedCdl
 */
  public TTracker(ZooKeeper zk, int data, String basePath, CountDownLatch startCdl,
                  CountDownLatch leaderChangeCdl, CountDownLatch leaveCdl, CountDownLatch dataChangedCdl) {
    this.basePath = basePath;
    this.data = data;
    this.zk = zk;
    this.startCdl = startCdl;
    this.leaderChangeCdl = leaderChangeCdl;
    this.leaveCdl = leaveCdl;
    this.dataChangedCdl = dataChangedCdl;
    member = new TMember();
    member.setData(this.data);
  }
  
  private ZooKeeper zk;
  
  private String basePath;
  
  private boolean orbExceptionEvent = false;
  private boolean newMemberEvent = false;
  private boolean lostMemberEvent = false;
  private boolean LeadershipChangeEvent = false;
  private boolean memberDataChangeEvent = false;
  
  private CountDownLatch startCdl;
  private CountDownLatch leaderChangeCdl;
  private CountDownLatch leaveCdl;
  private CountDownLatch dataChangedCdl;
  
  private int data;
  private TMember member;
  
  private Boolean shutdown = false;
  
  private LeaderGroup<TMember> leaderGroup;
  
/**
 * 
 */
  @Override
  public void run() {
    leaderGroup = new LeaderGroup<TMember>(zk, new OrbTTrackerCallback(), basePath, member, TMember.class);
    startCdl.countDown();
  }
  
/**
 * 
 */
  public void shutdown() {
    synchronized (shutdown) {
      shutdown = true;
    }
  }
  
  public class OrbTTrackerCallback implements OrbCallback {
    
/**
 * 
 * @param  OrbEvent e
 */
    public void process(OrbEvent e) {
      if (e.getClass() == OrbExceptionEvent.class) {
        ((OrbExceptionEvent)e).getException().printStackTrace();
        orbExceptionEvent = true;
      }
      if (e.getClass() == NewMemberEvent.class) {
        System.out.println("A new member joined the LeaderGroup");
        orbExceptionEvent = true;
      }
      if (e.getClass() == LostMemberEvent.class) {
        System.out.println("A member has been lost from the LeaderGroup");
        orbExceptionEvent = true;
      }
      if (e.getClass() == LeadershipChangeEvent.class) {
        System.out.println("Leadership has changed hands");
        leaderChangeCdl.countDown();
        orbExceptionEvent = true;
      }
      if (e.getClass() == MemberDataChangeEvent.class) {
        memberDataChangeEvent = true;
        //System.err.println("Received update on TTracker: " + data);
        dataChangedCdl.countDown();
      }
    }
    
  }
  
/**
 * Return the rbExceptionEvent
 */
  public boolean isOrbExceptionEvent() {
    return orbExceptionEvent;
  }
  
/**
 * Set the orbExceptionEvent
 * @param  boolean orbExceptionEvent
 */
  public void setOrbExceptionEvent(boolean orbExceptionEvent) {
    this.orbExceptionEvent = orbExceptionEvent;
  }
  
/**
 * Return the ewMemberEvent
 */
  public boolean isNewMemberEvent() {
    return newMemberEvent;
  }
  
/**
 * Set the newMemberEvent
 * @param  boolean newMemberEvent
 */
  public void setNewMemberEvent(boolean newMemberEvent) {
    this.newMemberEvent = newMemberEvent;
  }
  
/**
 * Return the ostMemberEvent
 */
  public boolean isLostMemberEvent() {
    return lostMemberEvent;
  }
  
/**
 * Set the lostMemberEvent
 * @param  boolean lostMemberEvent
 */
  public void setLostMemberEvent(boolean lostMemberEvent) {
    this.lostMemberEvent = lostMemberEvent;
  }
  
/**
 * Return the eadershipChangeEvent
 */
  public boolean isLeadershipChangeEvent() {
    return LeadershipChangeEvent;
  }
  
/**
 * Set the leadershipChangeEvent
 * @param  boolean leadershipChangeEvent
 */
  public void setLeadershipChangeEvent(boolean leadershipChangeEvent) {
    LeadershipChangeEvent = leadershipChangeEvent;
  }
  
/**
 * Return the eader
 */
  public boolean isLeader(){
    return leaderGroup.isLeader();
  }
  
/**
 * Return the emberDataChangeEvent
 */
  public boolean isMemberDataChangeEvent() {
    return memberDataChangeEvent;
  }
  
/**
 * 
 */
  public void leave(){
    leaveCdl.countDown();
    leaderGroup.leave();
  }
  
/**
 * Return the leader
 */
  public TMember getLeader(){
    return leaderGroup.getLeader();
  }
  
/**
 * 
 * @param  int newData
 */
  public void changeMemberData(int newData) throws OrbZKFailure {
    member.changeData(newData, zk, getMyPath());
  }
  
/**
 * Return the membersPath
 */
  public List<String> getMembersPath() {
    return leaderGroup.getMembersPath();
  }
  
/**
 * Return the members
 */
  public Collection<TMember> getMembers() {
    return leaderGroup.getMembers();
  }
  
/**
 * Return the myPath
 */
  public String getMyPath() {
    return leaderGroup.getMyPath();
  }
  
/**
 * Return the memberData
 */
  public int getMemberData() {
    return member.getData();
  }
  
}
