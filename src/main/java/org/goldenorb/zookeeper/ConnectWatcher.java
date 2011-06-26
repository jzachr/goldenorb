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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * This class defines a ConnectWatcher, which is used to place watches for member nodes within the ZooKeeper
 * substructure.
 * 
 */
public class ConnectWatcher implements Watcher {
  
  private static final int SESSION_TIMEOUT = 10000;
  private CountDownLatch connectedSignal = new CountDownLatch(1);
  
/**
 * 
 * @param  String hosts
 * @returns ZooKeeper
 */
  public ZooKeeper connect(String hosts) throws IOException, InterruptedException {
    ZooKeeper _zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
    connectedSignal.await();
    return _zk;
  }
  
/**
 * 
 * @param  WatchedEvent event
 */
  public void process(WatchedEvent event) {
    if (event.getState() == KeeperState.SyncConnected) {
      connectedSignal.countDown();
    }
  }
}
