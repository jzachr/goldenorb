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
package org.goldenorb.zookeeper;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;
import org.junit.Test;

/**
 * Tests OrbBarrier by using Threads to simulate multiple members joining under a barrier in ZooKeeper.
 * 
 * @author long
 * 
 */
public class OrbBarrierTest {
  
  OrbConfiguration orbConf = new OrbConfiguration(true);
  String barrierName = "/TestBarrierName";
  CountDownLatch startLatch = new CountDownLatch(1);
  int numOfMembers;
  
  /**
   * Tests the behavior of the barrier if all expected member nodes join in a timely manner.
   * 
   * @throws Exception
   */
  @Test
  public void allMembersJoin() throws Exception {
    numOfMembers = 3;
    CountDownLatch everyoneDoneLatch = new CountDownLatch(numOfMembers);
    ZooKeeper zk = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    
    OrbBarrier testBarrier1 = new OrbBarrier(orbConf, barrierName, numOfMembers, "member1", zk);
    OrbBarrier testBarrier2 = new OrbBarrier(orbConf, barrierName, numOfMembers, "member2", zk);
    OrbBarrier testBarrier3 = new OrbBarrier(orbConf, barrierName, numOfMembers, "member3", zk);
    
    BarrierThread bThread1 = new BarrierThread(testBarrier1, startLatch, everyoneDoneLatch);
    BarrierThread bThread2 = new BarrierThread(testBarrier2, startLatch, everyoneDoneLatch);
    BarrierThread bThread3 = new BarrierThread(testBarrier3, startLatch, everyoneDoneLatch);
    
    bThread1.start();
    bThread2.start();
    bThread3.start();
    
    startLatch.countDown(); // start all threads
    
    everyoneDoneLatch.await(); // wait until all threads are done
    
    assertTrue(zk.exists(barrierName + "/member1", false) != null);
    assertTrue(zk.exists(barrierName + "/member2", false) != null);
    assertTrue(zk.exists(barrierName + "/member3", false) != null);
    
    ZookeeperUtils.deleteNodeIfEmpty(zk, barrierName + "/member1");
    ZookeeperUtils.deleteNodeIfEmpty(zk, barrierName + "/member2");
    ZookeeperUtils.deleteNodeIfEmpty(zk, barrierName + "/member3");
    ZookeeperUtils.deleteNodeIfEmpty(zk, barrierName);
    zk.close();
  }
  
  /**
   * Tests the behavior of the barrier if only some of the member nodes join.
   * 
   * @throws Exception
   */
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
    
    everyoneDoneLatch.await(500, TimeUnit.MILLISECONDS); // wait on the threads with 500ms timeout, expect to
                                                         // timeout
    lastMemberLatch.countDown(); // start the last member
    
    everyoneDoneLatch.await();
    
    assertTrue(zk.exists(barrierName + "/member1", false) != null);
    assertTrue(zk.exists(barrierName + "/member2", false) != null);
    assertTrue(zk.exists(barrierName + "/member3", false) != null);
    
    ZookeeperUtils.deleteNodeIfEmpty(zk, barrierName + "/member1");
    ZookeeperUtils.deleteNodeIfEmpty(zk, barrierName + "/member2");
    ZookeeperUtils.deleteNodeIfEmpty(zk, barrierName + "/member3");
    ZookeeperUtils.deleteNodeIfEmpty(zk, barrierName);
    zk.close();
  }
  
  /**
   * This class defines a Thread that is used to start a member and have it enter the barrier.
   * 
   * @author long
   * 
   */
  class BarrierThread extends Thread {
    
    private OrbBarrier testBarrier;
    private CountDownLatch startLatch;
    private CountDownLatch everyoneDoneLatch;
    
    /**
     * Constructs the BarrierThread.
     * 
     * @param barrier
     *          - name of the OrbBarrier
     * @param startLatch
     *          - Used in conjunction with countDown within the calling block of code to start Threads
     * @param everyoneDoneLatch
     *          - Used to wait for all Threads to finish
     */
    public BarrierThread(OrbBarrier barrier, CountDownLatch startLatch, CountDownLatch everyoneDoneLatch) {
      this.testBarrier = barrier;
      this.startLatch = startLatch;
      this.everyoneDoneLatch = everyoneDoneLatch;
    }
    
    /**
     * Runs the Thread to enter the barrier. It first awaits the latch to start, then enters, and finally
     * counts down the everyoneDoneLatch once it is finished.
     */
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
