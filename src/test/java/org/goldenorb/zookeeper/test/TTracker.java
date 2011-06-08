package org.goldenorb.zookeeper.test;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.event.LeadershipChangeEvent;
import org.goldenorb.event.LostMemberEvent;
import org.goldenorb.event.NewMemberEvent;
import org.goldenorb.event.OrbCallback;
import org.goldenorb.event.OrbEvent;
import org.goldenorb.event.OrbExceptionEvent;
import org.goldenorb.zookeeper.LeaderGroup;

public class TTracker implements Runnable {
  
  public TTracker(ZooKeeper zk, int data, String basePath, CountDownLatch startCdl, CountDownLatch leaderChangeCdl, CountDownLatch leaveCdl) {
    this.basePath = basePath;
    this.data = data;
    this.zk = zk;
    this.startCdl = startCdl;
    this.leaderChangeCdl = leaderChangeCdl;
    this.leaveCdl = leaveCdl;
    member = new TMember();
    member.setData(this.data);
  }
  
  private ZooKeeper zk;
  
  private String basePath;
  
  private boolean orbExceptionEvent = false;
  private boolean newMemberEvent = false;
  private boolean lostMemberEvent = false;
  private boolean LeadershipChangeEvent = false;
  
  private CountDownLatch startCdl;
  private CountDownLatch leaderChangeCdl;
  private CountDownLatch leaveCdl;
  
  private int data;
  private TMember member;
  
  private Boolean shutdown = false;
  
  private LeaderGroup<TMember> leaderGroup;
  
  @Override
  public void run() {
    leaderGroup = new LeaderGroup<TMember>(zk, new OrbTTrackerCallback(), basePath, member);
    startCdl.countDown();
  }
  
  public void shutdown() {
    synchronized (shutdown) {
      shutdown = true;
    }
  }
  
  public class OrbTTrackerCallback implements OrbCallback {
    
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
    }
    
  }
  
  public boolean isOrbExceptionEvent() {
    return orbExceptionEvent;
  }
  
  public void setOrbExceptionEvent(boolean orbExceptionEvent) {
    this.orbExceptionEvent = orbExceptionEvent;
  }
  
  public boolean isNewMemberEvent() {
    return newMemberEvent;
  }
  
  public void setNewMemberEvent(boolean newMemberEvent) {
    this.newMemberEvent = newMemberEvent;
  }
  
  public boolean isLostMemberEvent() {
    return lostMemberEvent;
  }
  
  public void setLostMemberEvent(boolean lostMemberEvent) {
    this.lostMemberEvent = lostMemberEvent;
  }
  
  public boolean isLeadershipChangeEvent() {
    return LeadershipChangeEvent;
  }
  
  public void setLeadershipChangeEvent(boolean leadershipChangeEvent) {
    LeadershipChangeEvent = leadershipChangeEvent;
  }
  
  public boolean isLeader(){
    return leaderGroup.isLeader();
  }
  
  public void leave(){
    leaveCdl.countDown();
    leaderGroup.leave();
  }
  
  public TMember getLeader(){
    return leaderGroup.getLeader();
  }
}
