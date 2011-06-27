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

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;

/**
 * This class provides the implementation of a ZooKeeper Barrier for the GoldenOrb project. It can be used to
 * sync its constituent members before and after a computation or can be used at startup to wait for all
 * members to initialize and enter. This version implements an O(n^2) algorithm.
 * 
 */
public class OrbBarrier implements Barrier {
  
  private OrbConfiguration orbConf;
  String barrierName;
  int numOfMembers;
  String member;
  ZooKeeper zk;
  
  /**
   * Constructs an OrbBarrier object.
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
  public OrbBarrier(OrbConfiguration orbConf,
                    String barrierName,
                    int numOfMembers,
                    String member,
                    ZooKeeper zk) {
    this.orbConf = orbConf;
    this.barrierName = barrierName;
    this.numOfMembers = numOfMembers;
    this.member = member;
    this.zk = zk;
  }
  
  /**
   * This method creates a new member node under the barrier node if it does not already exist. It currently
   * is implemented with an O(n^2) algorithm where all members periodically check if the others have joined.
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
    
    ZookeeperUtils.tryToCreateNode(zk, barrierName, CreateMode.PERSISTENT);
    ZookeeperUtils.tryToCreateNode(zk, memberPath, CreateMode.EPHEMERAL);
    
    try {
      BarrierWait bw = new BarrierWait(this);
      List<String> list = zk.getChildren(barrierName, bw);
      
      // O(N^2) implementation
      while (list.size() < numOfMembers) {
        synchronized (this) {
          wait(2000);
          list = zk.getChildren(barrierName, bw);
        }
      }
      
    } catch (InterruptedException e) {
      throw new OrbZKFailure(e);
    } catch (KeeperException e) {
      throw new OrbZKFailure(e);
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
  
  /**
   * This class implements a Watcher for usage in the barrier mechanism for ZooKeeper.
   * 
   */
  class BarrierWait implements Watcher {
    OrbBarrier ob;
    
    /**
     * This constructs a BarrierWait object given a configured OrbBarrier object.
     * 
     * @param ob
     */
    public BarrierWait(OrbBarrier ob) {
      this.ob = ob;
    }
    
    /**
     * This method processes notifications triggered by Watchers.
     */
    @Override
    public void process(WatchedEvent event) {
      synchronized (ob) {
        ob.notify();
      }
    }
    
  }
}
