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

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;

/**
 * An implementation of a ZooKeeper barrier.  When all members have joined the barrier, either an
 * AllClear or AllDone node is created to signal it is safe to exit the barrier.  In general, AllClear is
 * used to signify it is safe to move past this barrier, but what ever processes have created the barrier need to
 * continue calculations once exiting this barrier.  The AllDone is used to signify if it is safe to move past the
 * barrier and if all of the processes that created the barrier are done with their calculations. It uses the same
 * O(n) algorithm that OrbFastBarrier does.
 * 
 */
public class OrbFastAllDoneBarrier implements AllDoneBarrier {
  
  private OrbConfiguration orbConf;
  private String barrierName;
  int numOfMembers;
  private String member;
  private ZooKeeper zk;
  private boolean active;
  
  /**
   * Constructs an OrbFastBarrier object.
   * 
   * @param orbConf
   *          - OrbConfiguration
   * @param barrierName
   *          - The barrier's name
   * @param numOfMembers
   *          - The total number of expected members to join under the barrier node
   * @param member
   *          - A member node's name
   * @param zk
   *          - ZooKeeper object
   */
  public OrbFastAllDoneBarrier(OrbConfiguration orbConf,
                        String barrierName,
                        int numOfMembers,
                        String member,
                        ZooKeeper zk) {
    this.orbConf = orbConf;
    this.barrierName = barrierName;
    this.numOfMembers = numOfMembers;
    this.member = member;
    this.zk = zk;
    this.active = true;
  }
  
  /**
   * This method creates a new member node under the barrier node if it does not already exist. It uses a O(n)
   * algorithm.
   * 
   * @exception InterruptedException
   *              throws OrbZKFailure
   * @exception KeeperException
   *              throws OrbZKFailure
   */
  @Override
  public boolean enter(boolean iAmDone) throws OrbZKFailure {
    // general path looks like: "/barrierName/member"
    String memberPath = null;
    if(iAmDone){
      memberPath = barrierName + "/" + member + "DONE";
    } else {
      memberPath = barrierName + "/" + member;
    }
     
    boolean notAllDone = true;
    
    /*
     * If this barrier is the first to enter() it will create the barrier node and firstToEnter will be the
     * path of the barrier node. Otherwise firstToEnter will equal null.
     */
    String firstToEnter = ZookeeperUtils.tryToCreateNode(zk, barrierName, CreateMode.PERSISTENT);
    ZookeeperUtils.tryToCreateNode(zk, memberPath, CreateMode.EPHEMERAL);
    
    if (firstToEnter != null) { // becomes the counter for this barrier
      try {
        BarrierWatcher bw = new BarrierWatcher(this);
        List<String> memberList = zk.getChildren(barrierName, bw);
        synchronized (this) {
          while (memberList.size() < numOfMembers) {
            // synchronized(this) {
            this.wait(1000);
            memberList = zk.getChildren(barrierName, bw);
          }
        }
        notAllDone = false;
        for(String member: memberList){
          if(!member.endsWith("DONE")){
            notAllDone = true;
          }
        }
        // Everyone has joined, give the All Clear to move forward
        if(notAllDone){
          ZookeeperUtils.tryToCreateNode(zk, barrierName + "/AllClear", CreateMode.EPHEMERAL);
        } else {
          ZookeeperUtils.tryToCreateNode(zk, barrierName + "/AllDone", CreateMode.EPHEMERAL);
        }
        // delete its node on they way out
        ZookeeperUtils.deleteNodeIfEmpty(zk, memberPath);
      } catch (KeeperException e) {
        e.printStackTrace();
        throw new OrbZKFailure(e);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new OrbZKFailure(e);
      }
    } else { // not first to enter, therefore just watches for the AllClear node
      try {
        BarrierWatcher bw = new BarrierWatcher(this);
        boolean notAllClear = true;
        notAllDone = true;
        notAllDone = (zk.exists(barrierName + "/AllDone", bw) == null);
        notAllClear = (zk.exists(barrierName + "/AllClear", bw) == null);
        while (notAllClear && notAllDone) {
          synchronized (this) {
            this.wait(1000);
          }
          notAllDone = (zk.exists(barrierName + "/AllDone", bw) == null);
          notAllClear = (zk.exists(barrierName + "/AllClear", bw) == null);
        }
        // delete its node on they way out
        ZookeeperUtils.deleteNodeIfEmpty(zk, memberPath);
      } catch (KeeperException e) {
        e.printStackTrace();
        throw new OrbZKFailure(e);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new OrbZKFailure(e);
      }
    }
    return !notAllDone;
  }
  
/**
 * 
 */
  public void makeInactive() {
    this.active = false;
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
   * This class implements a Watcher for usage in the barrier mechanism for ZooKeeper.
   * 
   */
  class BarrierWatcher implements Watcher {
    OrbFastAllDoneBarrier ofb;
    
    /**
     * This constructs a BarrierWatcher object given a configured OrbFastBarrier object.
     * 
     * @param orbFastBarrier
     */
    public BarrierWatcher(OrbFastAllDoneBarrier orbFastBarrier) {
      this.ofb = orbFastBarrier;
    }
    
    /**
     * This method processes notifications triggered by Watchers.
     */
    @Override
    public synchronized void process(WatchedEvent event) {
      synchronized (ofb) {
        if (OrbFastAllDoneBarrier.this.active) {
          ofb.notify();
        }
      }
    }
    
  }
  
}
