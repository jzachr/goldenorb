package org.goldenorb.zookeeper;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;
import org.junit.Test;

public class OrbBarrierTest {
  
  OrbConfiguration orbConf = new OrbConfiguration(true);
  String barrierName = "TestBarrierName";
  CountDownLatch startLatch = new CountDownLatch(1);
  int numOfMembers;
  
  @Test
  public void allMembersJoin() throws Exception {
    numOfMembers = 3;
    CountDownLatch everyoneDoneLatch = new CountDownLatch(numOfMembers);
    ZooKeeper zk = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    
    OrbBarrier testReef1 = new OrbBarrier(orbConf, barrierName, numOfMembers, "member1", zk);
    OrbBarrier testReef2 = new OrbBarrier(orbConf, barrierName, numOfMembers, "member2", zk);
    OrbBarrier testReef3 = new OrbBarrier(orbConf, barrierName, numOfMembers, "member3", zk);
    
    BarrierThread bThread1 = new BarrierThread(testReef1, startLatch, everyoneDoneLatch);
    BarrierThread bThread2 = new BarrierThread(testReef2, startLatch, everyoneDoneLatch);
    BarrierThread bThread3 = new BarrierThread(testReef3, startLatch, everyoneDoneLatch);
    
    bThread1.start();
    bThread2.start();
    bThread3.start();
    
    startLatch.countDown(); // start all threads
    
    everyoneDoneLatch.await(); // wait until all threads are done
    
    assertTrue(zk.exists("/" + barrierName + "/member1", false) != null);
    assertTrue(zk.exists("/" + barrierName + "/member2", false) != null);
    assertTrue(zk.exists("/" + barrierName + "/member3", false) != null);
    
    ZookeeperUtils.deleteNodeIfEmpty(zk, "/" + barrierName + "/member1");
    ZookeeperUtils.deleteNodeIfEmpty(zk, "/" + barrierName + "/member2");
    ZookeeperUtils.deleteNodeIfEmpty(zk, "/" + barrierName + "/member3");
    ZookeeperUtils.deleteNodeIfEmpty(zk, "/" + barrierName);
    zk.close();
  }
  
  @Test
  public void someMembersJoin() throws Exception {
    numOfMembers = 3;
    CountDownLatch everyoneDoneLatch = new CountDownLatch(numOfMembers);
    CountDownLatch lastMemberLatch = new CountDownLatch(1);
    ZooKeeper zk = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    
    OrbBarrier testBarrier1 = new OrbBarrier(orbConf, barrierName, numOfMembers, "member1", zk);
    OrbBarrier testBarrier2 = new OrbBarrier(orbConf, barrierName, numOfMembers, "member2", zk);
    OrbBarrier testBarrier3 = new OrbBarrier(orbConf, barrierName, numOfMembers, "member3", zk);
    
    BarrierThread bThread1 = new BarrierThread(testBarrier1, startLatch, everyoneDoneLatch);
    BarrierThread bThread2 = new BarrierThread(testBarrier2, startLatch, everyoneDoneLatch);
    BarrierThread bThread3 = new BarrierThread(testBarrier3, lastMemberLatch, everyoneDoneLatch);
    
    bThread1.start();
    bThread2.start();
    bThread3.start();
    
    startLatch.countDown(); // start first 2 threads
    
    everyoneDoneLatch.await(500, TimeUnit.MILLISECONDS); // wait on the threads with 500ms timeout, expect to timeout
    lastMemberLatch.countDown(); // start the last member
    
    everyoneDoneLatch.await();
    
    assertTrue(zk.exists("/" + barrierName + "/member1", false) != null);
    assertTrue(zk.exists("/" + barrierName + "/member2", false) != null);
    assertTrue(zk.exists("/" + barrierName + "/member3", false) != null);
    
    ZookeeperUtils.deleteNodeIfEmpty(zk, "/" + barrierName + "/member1");
    ZookeeperUtils.deleteNodeIfEmpty(zk, "/" + barrierName + "/member2");
    ZookeeperUtils.deleteNodeIfEmpty(zk, "/" + barrierName);
    zk.close();
  }
  
  class BarrierThread extends Thread {
    
    private OrbBarrier testBarrier;
    private CountDownLatch startLatch;
    private CountDownLatch everyoneDoneLatch;
    
    public BarrierThread(OrbBarrier barrier, CountDownLatch startLatch, CountDownLatch everyoneDoneLatch) {
      this.testBarrier = barrier;
      this.startLatch = startLatch;
      this.everyoneDoneLatch = everyoneDoneLatch;
    }
    
    public void run() {
      try {
        startLatch.await(); // wait for CountDown signal
        testBarrier.enter();
        everyoneDoneLatch.countDown(); // thread completed
      } catch (OrbZKFailure e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
  }
  
}
