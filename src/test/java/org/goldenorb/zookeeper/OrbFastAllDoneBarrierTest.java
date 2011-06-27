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

package org.goldenorb.zookeeper;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;
import org.junit.Test;

public class OrbFastAllDoneBarrierTest {

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
    
    OrbFastAllDoneBarrier testBarrier1 = new OrbFastAllDoneBarrier(orbConf, barrierName, numOfMembers, "member1", zk);
    OrbFastAllDoneBarrier testBarrier2 = new OrbFastAllDoneBarrier(orbConf, barrierName, numOfMembers, "member2", zk);
    OrbFastAllDoneBarrier testBarrier3 = new OrbFastAllDoneBarrier(orbConf, barrierName, numOfMembers, "member3", zk);
    
    BarrierThread bThread1 = new BarrierThread(testBarrier1, startLatch, everyoneDoneLatch, true);
    BarrierThread bThread2 = new BarrierThread(testBarrier2, startLatch, everyoneDoneLatch, true);
    BarrierThread bThread3 = new BarrierThread(testBarrier3, startLatch, everyoneDoneLatch, true);
    
    bThread1.start();
    bThread2.start();
    bThread3.start();
    
    startLatch.countDown(); // start all threads
    
    everyoneDoneLatch.await(); // wait until all threads are done
    
    // the nodes have deleted themselves ont the way out
    assertTrue(zk.exists(barrierName + "/member1", false) == null);
    assertTrue(zk.exists(barrierName + "/member2", false) == null);
    assertTrue(zk.exists(barrierName + "/member3", false) == null);
    
    // the nodes say they are all done
    assertTrue(bThread1.allDone());
    assertTrue(bThread2.allDone());
    assertTrue(bThread3.allDone());
    // the AllDone node exits
    assertTrue(zk.exists(barrierName + "/AllDone", false) != null);
    
    ZookeeperUtils.recursiveDelete(zk, barrierName);
    ZookeeperUtils.deleteNodeIfEmpty(zk, barrierName);
  }
  
  /**
   * Tests the behavior of the barrier if all expected member nodes join in a timely manner.
   * 
   * @throws Exception
   */
  @Test
  public void allMembersJoinNotAllDone() throws Exception {
    numOfMembers = 3;
    CountDownLatch everyoneDoneLatch = new CountDownLatch(numOfMembers);
    startLatch = new CountDownLatch(1);
    ZooKeeper zk = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    
    OrbFastAllDoneBarrier testBarrier1 = new OrbFastAllDoneBarrier(orbConf, barrierName, numOfMembers, "member1", zk);
    OrbFastAllDoneBarrier testBarrier2 = new OrbFastAllDoneBarrier(orbConf, barrierName, numOfMembers, "member2", zk);
    OrbFastAllDoneBarrier testBarrier3 = new OrbFastAllDoneBarrier(orbConf, barrierName, numOfMembers, "member3", zk);
    
    BarrierThread bThread1 = new BarrierThread(testBarrier1, startLatch, everyoneDoneLatch, true);
    BarrierThread bThread2 = new BarrierThread(testBarrier2, startLatch, everyoneDoneLatch, true);
    BarrierThread bThread3 = new BarrierThread(testBarrier3, startLatch, everyoneDoneLatch, false);
    
    bThread1.start();
    bThread2.start();
    bThread3.start();
    
    startLatch.countDown(); // start all threads
    
    everyoneDoneLatch.await(); // wait until all threads are done
    
    // the nodes have deleted themselves ont the way out
    assertTrue(zk.exists(barrierName + "/member1", false) == null);
    assertTrue(zk.exists(barrierName + "/member2", false) == null);
    assertTrue(zk.exists(barrierName + "/member3", false) == null);
    
    // the nodes say they are all done
    assertTrue(!bThread1.allDone());
    assertTrue(!bThread2.allDone());
    assertTrue(!bThread3.allDone());
    // the AllDone node exits
    assertTrue(zk.exists(barrierName + "/AllClear", false) != null);
    
    ZookeeperUtils.recursiveDelete(zk, barrierName);
    ZookeeperUtils.deleteNodeIfEmpty(zk, barrierName);
  }
  
  /**
   * Tests the behavior of the barrier if only some of the member nodes join.
   * 
   * @throws Exception
   */
  @Test
  public void someMembersJoin() throws Exception {
    numOfMembers = 3;
    startLatch = new CountDownLatch(1);
    CountDownLatch everyoneDoneLatch = new CountDownLatch(numOfMembers);
    CountDownLatch lastMemberLatch = new CountDownLatch(1);
    ZooKeeper zk = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    
    OrbFastAllDoneBarrier testBarrier1 = new OrbFastAllDoneBarrier(orbConf, barrierName, numOfMembers, "member1", zk);
    OrbFastAllDoneBarrier testBarrier2 = new OrbFastAllDoneBarrier(orbConf, barrierName, numOfMembers, "member2", zk);
    OrbFastAllDoneBarrier testBarrier3 = new OrbFastAllDoneBarrier(orbConf, barrierName, numOfMembers, "member3", zk);
    
    BarrierThread bThread1 = new BarrierThread(testBarrier1, startLatch, everyoneDoneLatch, true);
    BarrierThread bThread2 = new BarrierThread(testBarrier2, startLatch, everyoneDoneLatch, true);
    BarrierThread bThread3 = new BarrierThread(testBarrier3, lastMemberLatch, everyoneDoneLatch, false);
    
    bThread1.start();
    bThread2.start();
    bThread3.start();
    
    startLatch.countDown(); // start first 2 threads
    
    everyoneDoneLatch.await(500, TimeUnit.MILLISECONDS); // wait on the threads with 500ms timeout, expect to
                                                         // timeout
    
    
    
    
    assertTrue((zk.exists(barrierName + "/member1DONE", false) != null)||(zk.exists(barrierName + "/member1", false) != null));
    assertTrue((zk.exists(barrierName + "/member2DONE", false) != null)||(zk.exists(barrierName + "/member2", false) != null));
    
    lastMemberLatch.countDown(); // start the last member
    everyoneDoneLatch.await();
    assertTrue(zk.exists(barrierName + "/AllClear", false) != null);
    
    testBarrier1.makeInactive();
    testBarrier2.makeInactive();
    ZookeeperUtils.recursiveDelete(zk, barrierName);
    ZookeeperUtils.deleteNodeIfEmpty(zk, barrierName);
    zk.close();
  }
  
  /**
   * Tests behavior of orbFastBarrier over groups of a large number and a larger number of steps
   * @throws InterruptedException 
   * @throws IOException 
   * @throws OrbZKFailure 
   */
  @Test
  public void StressTest() throws IOException, InterruptedException, OrbZKFailure {
    //deleteThreads();
    int numBarrierStressThreads = 25;
    int numSteps = 25;
    CountDownLatch complete = new CountDownLatch(numBarrierStressThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    BarrierStressThread[] threads = new BarrierStressThread[numBarrierStressThreads];
    ZooKeeper zk = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    for(int i =0; i < numBarrierStressThreads; i++) {
      ZookeeperUtils.recursiveDelete(zk, "/barrier"+i);
      ZookeeperUtils.deleteNodeIfEmpty(zk, "/barrier"+i);
      threads[i] = new BarrierStressThread(numSteps, complete, startLatch, numBarrierStressThreads, zk, orbConf, "member"+i);
      threads[i].start();
    }
    startLatch.countDown();
    complete.await();
    boolean allBarriersCreated = true;
    for(int i=0; i < numBarrierStressThreads; i++) {
      allBarriersCreated = allBarriersCreated && ZookeeperUtils.nodeExists(zk, "/barrier"+i);
      if (allBarriersCreated) {
        ZookeeperUtils.recursiveDelete(zk, "/barrier"+i);
        ZookeeperUtils.deleteNodeIfEmpty(zk, "/barrier"+i);
      }
    }
    assertTrue(allBarriersCreated);
  }
  
/**
 * 
 */
  private void deleteThreads() throws InterruptedException, OrbZKFailure, IOException {
    ZooKeeper zk = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    for(int i=0; i < 10; i++) {
      ZookeeperUtils.recursiveDelete(zk, "/barrier"+i);
      ZookeeperUtils.deleteNodeIfEmpty(zk, "/barrier"+i);
    }
    System.err.println("Waiting 10 seconds");
    Thread.sleep(10000);
  }

  /**
   * This class defines a Thread that is used to start a member and have it enter the barrier.
   * 
   * @author long
   * 
   */
  class BarrierThread extends Thread {
    
    private OrbFastAllDoneBarrier testBarrier;
    private CountDownLatch startLatch;
    private CountDownLatch everyoneDoneLatch;
    private boolean allDone;
    private boolean isDone;
    
    /**
     * Constructs the BarrierThread.
     * 
     * @param barrier
     *          - name of the OrbFastBarrier
     * @param startLatch
     *          - Used in conjunction with countDown within the calling block of code to start Threads
     * @param everyoneDoneLatch
     *          - Used to wait for all Threads to finish
     */
    public BarrierThread(OrbFastAllDoneBarrier barrier, CountDownLatch startLatch, CountDownLatch everyoneDoneLatch, boolean isDone) {
      this.testBarrier = barrier;
      this.startLatch = startLatch;
      this.everyoneDoneLatch = everyoneDoneLatch;
      this.allDone = false;
      this.isDone = isDone;
    }
    
    /**
     * Runs the Thread to enter the barrier. It first awaits the latch to start, then enters, and finally
     * counts down the everyoneDoneLatch once it is finished.
     */
    public void run() {
      try {
        startLatch.await(); // wait for CountDown signal
        allDone = testBarrier.enter(isDone);
        everyoneDoneLatch.countDown(); // thread completed
      } catch (OrbZKFailure e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    public boolean allDone() {
      return allDone;
    }
    
  }
  
  /**
   * This class defines a Thread that is used to start a member and have it enter the barrier.
   * 
   * @author long
   * 
   */
  class BarrierStressThread extends Thread {
    
    private int numSteps;
    private CountDownLatch everyoneDoneLatch;
    private int numBarrierStressThreads;
    private ZooKeeper zk;
    private OrbConfiguration orbConf;
    private CountDownLatch waitToStart;
    private String member;
    /**
     * Constructs the BarrierStressThread.
     * 
     * @param barrier
     *          - name of the OrbFastBarrier
     * @param numSteps
     *          - number of barriers thread will have to go through to complete
     * @param everyoneDoneLatch
     *          - Used to wait for all Threads to finish
     */
    public BarrierStressThread(int numSteps,
                               CountDownLatch everyoneDoneLatch,
                               CountDownLatch waitToStart,
                               int numBarrierStressThreads,
                               ZooKeeper zk,
                               OrbConfiguration orbConf,
                               String member) {
      this.numSteps = numSteps;
      this.everyoneDoneLatch = everyoneDoneLatch;
      this.numBarrierStressThreads = numBarrierStressThreads;
      this.zk = zk;
      this.orbConf = orbConf;
      this.waitToStart = waitToStart;
      this.member = member;
    }
    
    /**
     * Runs the Thread to enter the barrier. It first awaits the latch to start, then enters, and finally
     * counts down the everyoneDoneLatch once it is finished.
     */
    public void run() {
      try {
        waitToStart.await();
        for(int i=0; i < numSteps; i++) {
          if (i < numSteps -1) {
            OrbFastAllDoneBarrier ofb = new OrbFastAllDoneBarrier(orbConf, "/barrier"+i, numBarrierStressThreads, member, zk);
            assertTrue(!ofb.enter(false));
          } else {
            OrbFastAllDoneBarrier ofb = new OrbFastAllDoneBarrier(orbConf, "/barrier"+i, numBarrierStressThreads, member, zk);
            assertTrue(ofb.enter(true));
          }
        }
        everyoneDoneLatch.countDown(); // thread completed
      } catch (OrbZKFailure e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    
    
  }
  
}
