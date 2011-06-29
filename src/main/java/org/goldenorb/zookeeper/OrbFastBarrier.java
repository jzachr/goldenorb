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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides the implementation of a ZooKeeper Barrier for the GoldenOrb project. It can be used to
 * sync its constituent members before and after a computation or can be used at startup to wait for all
 * members to initialize and enter. OrbFastBarrier utilizes a O(n) algorithm for enter().
 * 
 */
public class OrbFastBarrier implements Barrier {
  
  private final Logger logger = LoggerFactory.getLogger(OrbFastBarrier.class);
  
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
  public OrbFastBarrier(OrbConfiguration orbConf,
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
  public void enter() throws OrbZKFailure {
    // general path looks like: "/barrierName/member"
    String memberPath = barrierName + "/" + member;
    logger.debug("enter(): {}", memberPath);
    /*
     * If this barrier is the first to enter() it will create the barrier node and firstToEnter will be the
     * path of the barrier node. Otherwise firstToEnter will equal null.
     */
    String firstToEnter = ZookeeperUtils.tryToCreateNode(zk, barrierName, CreateMode.PERSISTENT);
    ZookeeperUtils.tryToCreateNode(zk, memberPath, CreateMode.EPHEMERAL);
    
    if (firstToEnter != null) { // becomes the counter for this barrier
      logger.debug("{} is the counter", memberPath);
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
        logger.debug("all {} have joined, sending AllClear", memberList.size());
        // Everyone has joined, give the All Clear to move forward
        ZookeeperUtils.tryToCreateNode(zk, barrierName + "/AllClear", CreateMode.EPHEMERAL);
        // delete its node on they way out
        ZookeeperUtils.deleteNodeIfEmpty(zk, memberPath);
      } catch (KeeperException e) {
        throw new OrbZKFailure(e);
      } catch (InterruptedException e) {
        throw new OrbZKFailure(e);
      }
    } else { // not first to enter, therefore just watches for the AllClear node
      try {
        logger.debug("{} not first to enter, waiting", memberPath);
        BarrierWatcher bw = new BarrierWatcher(this);
        while (zk.exists(barrierName + "/AllClear", bw) == null) {
          synchronized (this) {
            this.wait(1000);
          }
        }
        logger.debug("{} recvd AllClear, moving on", memberPath);
        // delete its node on they way out
        ZookeeperUtils.deleteNodeIfEmpty(zk, memberPath);
      } catch (KeeperException e) {
        throw new OrbZKFailure(e);
      } catch (InterruptedException e) {
        throw new OrbZKFailure(e);
      }
    }
  }
  
  /**
   * Makes this inactive.
   */
  public void makeInactive() {
    this.active = false;
  }
  
  @Override
  public void setOrbConf(OrbConfiguration orbConf) {
    this.orbConf = orbConf;
  }
  
  @Override
  public OrbConfiguration getOrbConf() {
    return orbConf;
  }
  
  /**
   * This class implements a Watcher for usage in the barrier mechanism for ZooKeeper.
   * 
   */
  class BarrierWatcher implements Watcher {
    OrbFastBarrier ofb;
    
    /**
     * This constructs a BarrierWatcher object given a configured OrbFastBarrier object.
     * 
     * @param orbFastBarrier
     */
    public BarrierWatcher(OrbFastBarrier orbFastBarrier) {
      this.ofb = orbFastBarrier;
    }
    
    /**
     * This method processes notifications triggered by Watchers.
     */
    @Override
    public synchronized void process(WatchedEvent event) {
      synchronized (ofb) {
        if (OrbFastBarrier.this.active) {
          ofb.notify();
        }
      }
    }
    
  }
  
}
