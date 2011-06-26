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

import org.apache.zookeeper.*;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class OrbSimpleZKTest {
/**
 * 
 */
  @Test
  public void simpleTest() {
    assertTrue(true);
  }
  
/**
 * 
 */
  @Test
  public void connectToZooKeeper() throws IOException, InterruptedException{
    ZooKeeper zk = new ZooKeeper("localhost", 5000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        System.out.println(event);
      }
    });
    
    zk.close();
  }
}
