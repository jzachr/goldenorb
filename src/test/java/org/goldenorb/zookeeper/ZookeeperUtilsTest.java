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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

public class ZookeeperUtilsTest {
  
  private static Logger logger;
  private static OrbConfiguration orbConf = new OrbConfiguration(true);
  ZooKeeper ZK;
  
  @BeforeClass
  public static void setUp() {
    orbConf.setOrbZooKeeperQuorum("localhost:21810");
  }
  
  @Test
  public void test_connect() {
    assertNotNull(ZookeeperUtils.class);
    try {
      ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
      assertNotNull(ZK);
    } catch (IOException e) {
      logger.info("Failed to connect to zookeeper on " + orbConf.getOrbZooKeeperQuorum());
      logger.error("IOException", e);
    } catch (InterruptedException e) {
      logger.info("Failed to connect to zookeeper on " + orbConf.getOrbZooKeeperQuorum());
      logger.error("InterruptedException", e);
    }
  }
  
  @Test
  public void test_writableToByteArray() {
    byte[] byteArray;
    Text testInputText = new Text("this is a test text phrase");
    Text testOutputText = new Text();
    
    try {
      byteArray = ZookeeperUtils.writableToByteArray(testInputText);
      assertNotNull(byteArray);
      
      ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
      DataInput in = new DataInputStream(bais);
      testOutputText.readFields(in);
      assertEquals(testInputText, testOutputText);
    } catch (IOException e) {
      logger.error("Failed to convert Writable to byte array.");
    }
  }
  
  @Test
  public void test_byteArrayToWritable_Orb() {
    Text testInputText = new Text("test writable");
    Text testOutputText = new Text();
    
    try {
      byte[] byteArray = ZookeeperUtils.writableToByteArray(testInputText);
      testOutputText = (Text) ZookeeperUtils.byteArrayToWritable(byteArray, Text.class, orbConf);
      assertEquals(testInputText, testOutputText);
    } catch (IOException e) {
      logger.error("Failed to convert Writable to byte array.");
    }
    
  }
  
  @Test
  public void test_byteArrayToWritable() {
    Text testInputText = new Text("test writable");
    Text testOutputText = new Text();
    
    try {
      byte[] byteArray = ZookeeperUtils.writableToByteArray(testInputText);
      testOutputText = (Text) ZookeeperUtils.byteArrayToWritable(byteArray, testOutputText);
      assertEquals(testInputText, testOutputText);
    } catch (IOException e) {
      logger.error("Failed to convert Writable to byte array.");
    }
  }
  
  @Test
  public void test_nodeExists() throws IOException, InterruptedException, KeeperException {
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    ZK.create("/node", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    
    assertTrue(ZookeeperUtils.nodeExists(ZK, "/node"));
    assertFalse(ZookeeperUtils.nodeExists(ZK, "/falsenode"));
    
    ZK.delete("/node", -1);
  }
  
  @Test
  public void test_tryToCreateNode() throws IOException, InterruptedException, OrbZKFailure, KeeperException {
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    
    ZookeeperUtils.tryToCreateNode(ZK, "/node");
    assertTrue(ZK.exists("/node", false) != null);
    ZookeeperUtils.tryToCreateNode(ZK, "/node"); // throw a KeeperException.NodeExistsException
    
    ZK.delete("/node", -1);
  }
  
  @Test
  public void test_tryToCreateNode_CM() throws IOException,
                                       InterruptedException,
                                       OrbZKFailure,
                                       KeeperException {
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    
    ZookeeperUtils.tryToCreateNode(ZK, "/node", CreateMode.EPHEMERAL);
    assertTrue(ZK.exists("/node", false) != null);
    ZookeeperUtils.tryToCreateNode(ZK, "/node", CreateMode.EPHEMERAL); // throw a
                                                                       // KeeperException.NodeExistsException
    
    ZK.delete("/node", -1);
  }
  
  @Test
  public void test_tryToCreateNode_CM_Writable() throws IOException,
                                                InterruptedException,
                                                OrbZKFailure,
                                                KeeperException {
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    
    ZookeeperUtils.tryToCreateNode(ZK, "/node", new Text("test node"), CreateMode.EPHEMERAL);
    assertTrue(ZK.exists("/node", false) != null);
    ZookeeperUtils.tryToCreateNode(ZK, "/node", new Text("test node"), CreateMode.EPHEMERAL); // throw a
                                                                                              // KeeperException.NodeExistsException
    
    ZK.delete("/node", -1);
  }
  
  @Test
  public void test_notExistCreateNode() throws IOException,
                                       InterruptedException,
                                       KeeperException,
                                       OrbZKFailure {
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    
    ZookeeperUtils.notExistCreateNode(ZK, "/node");
    String nodePath = ZookeeperUtils.notExistCreateNode(ZK, "/node"); // try to create an existing node
    assertTrue(ZK.exists("/node", false) != null);
    assertTrue(nodePath == null);
    
    ZK.delete("/node", -1);
  }
  
  @Test
  public void test_notExistCreateNode_CM() throws IOException,
                                          InterruptedException,
                                          KeeperException,
                                          OrbZKFailure {
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    
    ZookeeperUtils.notExistCreateNode(ZK, "/node", CreateMode.EPHEMERAL);
    String nodePath = ZookeeperUtils.notExistCreateNode(ZK, "/node", CreateMode.EPHEMERAL); // try to create
                                                                                            // an existing
                                                                                            // node
    assertTrue(ZK.exists("/node", false) != null);
    assertTrue(nodePath == null);
    
    ZK.delete("/node", -1);
  }
  
  @Test
  public void test_notExistCreateNode_CM_Writable() throws IOException,
                                                   InterruptedException,
                                                   KeeperException,
                                                   OrbZKFailure {
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    
    ZookeeperUtils.notExistCreateNode(ZK, "/node", new Text("test node"), CreateMode.EPHEMERAL);
    String nodePath = ZookeeperUtils.notExistCreateNode(ZK, "/node", new Text("test node"),
      CreateMode.EPHEMERAL); // try to create an existing node
    assertTrue(ZK.exists("/node", false) != null);
    assertTrue(nodePath == null);
    
    ZK.delete("/node", -1);
  }
  
  @Test
  public void test_getNodeWritable() throws IOException, InterruptedException, KeeperException, OrbZKFailure {
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    Text input = new Text("test node writable");
    Text txt = new Text();
    
    ZK.create("/node", ZookeeperUtils.writableToByteArray(input), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    txt = (Text) ZookeeperUtils.getNodeWritable(ZK, "/node", Text.class, orbConf);
    ZookeeperUtils.getNodeWritable(ZK, "/falsenode", Text.class, orbConf); // throw exception
    assertEquals(input, txt);
    
    ZK.delete("/node", -1);
  }
  
  @Test
  public void test_getNodeWritable_Watcher() throws IOException,
                                            InterruptedException,
                                            KeeperException,
                                            OrbZKFailure {
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    Text input = new Text("test node writable");
    Text txt = new Text();
    
    ZK.create("/node", ZookeeperUtils.writableToByteArray(input), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    txt = (Text) ZookeeperUtils.getNodeWritable(ZK, "/node", Text.class, orbConf, new TestWatcher());
    ZookeeperUtils.getNodeWritable(ZK, "/falsenode", Text.class, orbConf, new TestWatcher()); // throw
                                                                                              // exception
    assertEquals(input, txt);
    
    ZK.delete("/node", -1);
  }
  
  @Test
  public void test_getNodeWritable_Watcher_Writable() throws IOException,
                                                     InterruptedException,
                                                     KeeperException,
                                                     OrbZKFailure {
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    Text input = new Text("test node writable");
    Text txt = new Text();
    
    ZK.create("/node", ZookeeperUtils.writableToByteArray(input), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    ZookeeperUtils.getNodeWritable(ZK, "/node", txt, new TestWatcher());
    ZookeeperUtils.getNodeWritable(ZK, "/falsenode", txt, new TestWatcher()); // throw exception
    assertEquals(input, txt);
    
    ZK.delete("/node", -1);
  }
  
  @Test
  public void test_deleteNodeIfEmpty() throws IOException,
                                      InterruptedException,
                                      KeeperException,
                                      OrbZKFailure {
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    
    ZK.create("/node", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    ZookeeperUtils.deleteNodeIfEmpty(ZK, "/node");
    assertTrue(ZK.exists("/node", false) == null);
    
    ZK.create("/parent", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    ZK.create("/parent/child", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    ZookeeperUtils.deleteNodeIfEmpty(ZK, "/parent"); // throws exception because /parent has children
    assertTrue(ZK.exists("/parent", false) != null);
    assertTrue(ZK.exists("/parent/child", false) != null);
    
    ZK.delete("/parent/child", -1);
    ZK.delete("/parent", -1);
  }
  
  @Test
  public void test_recursiveDelete() throws IOException, InterruptedException, KeeperException, OrbZKFailure {
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    
    ZK.create("/parent", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    ZK.create("/parent/child", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    ZK.create("/parent/child/grandchild", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    ZookeeperUtils.recursiveDelete(ZK, "/parent");
    assertTrue(ZK.getChildren("/parent", false).size() == 0);
    
    ZookeeperUtils.recursiveDelete(ZK, "/testInvalidPath");
    
    ZK.delete("/parent", -1);
  }
  
  @Test
  public void test_updateNodeData() throws IOException, InterruptedException, KeeperException, OrbZKFailure {
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    Text input = new Text("test node writable");
    
    ZK.create("/node", ZookeeperUtils.writableToByteArray(input), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    ZookeeperUtils.updateNodeData(ZK, "/node", new Text("updated node"));
    assertTrue(ZK.getData("/node", false, null) != ZookeeperUtils.writableToByteArray(input));
    
    ZookeeperUtils.updateNodeData(ZK, "/invalidUpdateNode", new Text("updated node")); // throw exception
    
    ZK.delete("/node", -1);
  }
  
  @Test
  public void test_existsUpdateNodeData() throws IOException,
                                         InterruptedException,
                                         KeeperException,
                                         OrbZKFailure {
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    Text txt = new Text();
    ZK.create("/node", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    ZookeeperUtils.existsUpdateNodeData(ZK, "/node", new Text("updated node"));
    ZookeeperUtils.getNodeWritable(ZK, "/node", txt, null);
    assertEquals(txt, new Text("updated node"));
    
    ZookeeperUtils.existsUpdateNodeData(ZK, "/invalidNode", new Text("updated node"));
    
    ZK.delete("/node", -1);
  }
  
  @Test
  public void test_getChildren() throws IOException, InterruptedException, KeeperException, OrbZKFailure {
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    
    ZK.create("/node", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    ZK.create("/node/child1", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    ZK.create("/node/child2", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    
    List<String> list = ZookeeperUtils.getChildren(ZK, "/node", null);
    assertTrue(list.size() == 2);
    
    ZK.delete("/node/child1", -1);
    ZK.delete("/node/child2", -1);
    ZK.delete("/node", -1);
  }
  
  class TestWatcher implements Watcher {
    
    public TestWatcher() {}
    
    @Override
    public void process(WatchedEvent event) {
      synchronized (this) {
        this.notify();
      }
    }
  }
}
