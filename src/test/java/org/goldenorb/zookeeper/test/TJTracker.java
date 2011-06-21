package org.goldenorb.zookeeper.test;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.JobManager;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.event.OrbCallback;
import org.goldenorb.event.OrbEvent;
import org.goldenorb.event.OrbExceptionEvent;
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
  
  @Override
  public void setOrbConf(OrbConfiguration orbConf) {
    this.orbConf = orbConf;
  }
  
  @Override
  public OrbConfiguration getOrbConf() {
    return orbConf;
  }
  
  private void leader() {
    synchronized (this) {
      leader = true;
      jobManager = new JobManager(orbCallback, orbConf, zk);
    }
    waitLoop();
  }
  
  private void slave() {
    synchronized (this) {
      leader = false;
      if (jobManager != null) {
        jobManager.shutdown();
      }
    }
    waitLoop();
  }
  
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
  
  public void leave() {
    runTracker = false;
    leaderGroup.leave();
    if (jobManager != null) {
      jobManager.shutdown();
    }
    exit.countDown();
  }

  
  
  public boolean isLeader() {
    return leaderGroup.isLeader();
  }
  
}
