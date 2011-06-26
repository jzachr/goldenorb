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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.goldenorb.conf.OrbConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains several static utility methods for GoldenOrb to interface with ZooKeeper.
 * 
 */
public class ZookeeperUtils {
  
  private static ConnectWatcher connect;
  private static Logger LOG = LoggerFactory.getLogger(ZookeeperUtils.class);
  
  /**
   * 
   * @param hosts
   *          - String
   * @returns ZooKeeper
   */
  public static ZooKeeper connect(String hosts) throws IOException, InterruptedException {
    if (connect == null) {
      connect = new ConnectWatcher();
    }
    return connect.connect(hosts);
  }
  
  /**
   * 
   * @param w
   *          - Writable
   * @returns byte[]
   */
  public static byte[] writableToByteArray(Writable w) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    w.write(out);
    return baos.toByteArray();
  }
  
  /**
   * 
   * @param byteArray
   *          - byte[]
   * @param writableClass
   *          - Class <? extends Writable>
   * @param orbConf
   *          - OrbConfiguration
   * @returns Writable
   */
  public static Writable byteArrayToWritable(byte[] byteArray,
                                             Class<? extends Writable> writableClass,
                                             OrbConfiguration orbConf) {
    ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
    DataInput in = new DataInputStream(bais);
    
    Writable w = (Writable) ReflectionUtils.newInstance(writableClass, orbConf);
    try {
      w.readFields(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return w;
  }
  
  /**
   * 
   * @param byteArray
   *          - byte[]
   * @param w
   *          - Writable
   * @returns Writable
   */
  public static Writable byteArrayToWritable(byte[] byteArray, Writable w) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
    DataInput in = new DataInputStream(bais);
    w.readFields(in);
    return w;
  }
  
  /**
   * 
   * @param zk
   *          - ZooKeeper
   * @param string
   *          - String
   * @returns boolean
   */
  public static boolean nodeExists(ZooKeeper zk, String string) {
    Stat stat;
    try {
      stat = zk.exists(string, false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return stat != null;
  }
  
  /**
   * 
   * @param zk
   *          - ZooKeeper
   * @param path
   *          - String
   * @returns String
   */
  public static String tryToCreateNode(ZooKeeper zk, String path) throws OrbZKFailure {
    String result = null;
    try {
      result = zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (KeeperException.NodeExistsException e) {
      LOG.debug("Node " + path + " already exists!");
    } catch (KeeperException e) {
      throw new OrbZKFailure(e);
    } catch (InterruptedException e) {
      throw new OrbZKFailure(e);
    }
    return result;
  }
  
  /**
   * 
   * @param zk
   *          - ZooKeeper
   * @param path
   *          - String
   * @param createMode
   *          - CreateMode
   * @returns String
   */
  public static String tryToCreateNode(ZooKeeper zk, String path, CreateMode createMode) throws OrbZKFailure {
    String result = null;
    try {
      result = zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, createMode);
    } catch (KeeperException.NodeExistsException e) {
      LOG.debug("Node " + path + " already exists!");
    } catch (KeeperException e) {
      throw new OrbZKFailure(e);
    } catch (InterruptedException e) {
      throw new OrbZKFailure(e);
    }
    return result;
  }
  
  /**
   * 
   * @param zk
   *          - ZooKeeper
   * @param path
   *          - String
   * @param node
   *          - Writable
   * @param createMode
   *          - CreateMode
   * @returns String
   */
  public static String tryToCreateNode(ZooKeeper zk, String path, Writable node, CreateMode createMode) throws OrbZKFailure {
    String result = null;
    try {
      result = zk.create(path, writableToByteArray(node), Ids.OPEN_ACL_UNSAFE, createMode);
    } catch (KeeperException.NodeExistsException e) {
      LOG.debug("Node " + path + " already exists!");
      System.err.println("Node " + path + " already exists!");
    } catch (KeeperException e) {
      e.printStackTrace();
      throw new OrbZKFailure(e);
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new OrbZKFailure(e);
    } catch (IOException e) {
      e.printStackTrace();
      throw new OrbZKFailure(e);
    }
    return result;
  }
  
  /**
   * 
   * @param zk
   *          - ZooKeeper
   * @param path
   *          - String
   * @returns String
   */
  public static String notExistCreateNode(ZooKeeper zk, String path) throws OrbZKFailure {
    String nodePath = null;
    if (!nodeExists(zk, path)) {
      nodePath = tryToCreateNode(zk, path);
    }
    return nodePath;
  }
  
  /**
   * 
   * @param zk
   *          - ZooKeeper
   * @param path
   *          - String
   * @param createMode
   *          - CreateMode
   * @returns String
   */
  public static String notExistCreateNode(ZooKeeper zk, String path, CreateMode createMode) throws OrbZKFailure {
    String nodePath = null;
    if (!nodeExists(zk, path)) {
      nodePath = tryToCreateNode(zk, path, createMode);
    }
    return nodePath;
  }
  
  /**
   * 
   * @param zk
   *          - ZooKeeper
   * @param path
   *          - String
   * @param node
   *          - Writable
   * @param createMode
   *          - CreateMode
   * @returns String
   */
  public static String notExistCreateNode(ZooKeeper zk, String path, Writable node, CreateMode createMode) throws OrbZKFailure {
    String nodePath = null;
    if (!nodeExists(zk, path)) {
      nodePath = tryToCreateNode(zk, path, node, createMode);
    }
    return nodePath;
  }
  
  /**
   * Return the nodeWritable
   */
  public static Writable getNodeWritable(ZooKeeper zk,
                                         String path,
                                         Class<? extends Writable> writableClass,
                                         OrbConfiguration orbConf) throws OrbZKFailure {
    byte[] result = null;
    try {
      result = zk.getData(path, false, null);
    } catch (KeeperException.NoNodeException e) {
      LOG.debug("Node " + path + " does not exist!");
    } catch (KeeperException e) {
      throw new OrbZKFailure(e);
    } catch (InterruptedException e) {
      throw new OrbZKFailure(e);
    }
    if (result != null) {
      return byteArrayToWritable(result, writableClass, orbConf);
    } else {
      return null;
    }
  }
  
  /**
   * Gets data from the Znode specified by path and sets a watcher.
   * 
   * @param zk
   * @param path
   * @param writableClass
   * @param orbConf
   * @param watcher
   * @return
   * @throws OrbZKFailure
   */
  public static Writable getNodeWritable(ZooKeeper zk,
                                         String path,
                                         Class<? extends Writable> writableClass,
                                         OrbConfiguration orbConf,
                                         Watcher watcher) throws OrbZKFailure {
    byte[] data = null;
    try {
      data = zk.getData(path, watcher, null);
    } catch (KeeperException.NoNodeException e) {
      LOG.debug("Node " + path + " does not exist!");
    } catch (KeeperException e) {
      throw new OrbZKFailure(e);
    } catch (InterruptedException e) {
      throw new OrbZKFailure(e);
    }
    if (data != null) {
      return byteArrayToWritable(data, writableClass, orbConf);
    } else {
      return null;
    }
  }
  
  /**
   * Gets data from the Znode specified by the path, reads it into the Writable and sets a watcher
   * 
   * @param zk
   * @param path
   *          is the path to the node
   * @param writable
   *          is the Writable object the node data will be read into
   * @param watcher
   *          is the Watcher object to set upon reading the node data
   * @return
   * @throws OrbZKFailure
   */
  public static Writable getNodeWritable(ZooKeeper zk, String path, Writable writable, Watcher watcher) throws OrbZKFailure {
    byte[] data = null;
    try {
      data = zk.getData(path, watcher, null);
    } catch (KeeperException.NoNodeException e) {
      LOG.debug("Node " + path + " does not exist!");
    } catch (KeeperException e) {
      throw new OrbZKFailure(e);
    } catch (InterruptedException e) {
      throw new OrbZKFailure(e);
    }
    if (data != null) {
      try {
        return byteArrayToWritable(data, writable);
      } catch (IOException e) {
        throw new OrbZKFailure(e);
      }
    }
    return null;
  }
  
  /**
   * 
   * @param zk
   *          - ZooKeeper
   * @param path
   *          - String
   */
  public static void deleteNodeIfEmpty(ZooKeeper zk, String path) throws OrbZKFailure {
    try {
      zk.delete(path, -1);
    } catch (KeeperException.NoNodeException e) {} catch (KeeperException.BadVersionException e) {
      e.printStackTrace();
    } catch (KeeperException.NotEmptyException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      throw new OrbZKFailure(e);
    } catch (KeeperException e) {
      throw new OrbZKFailure(e);
    }
  }
  
  /**
   * 
   * @param zk
   *          - ZooKeeper
   * @param path
   *          - String
   */
  public static void recursiveDelete(ZooKeeper zk, String path) throws OrbZKFailure {
    
    try {
      List<String> children = zk.getChildren(path, false);
      if (children != null) {
        for (String child : children) {
          recursiveDelete(zk, path + "/" + child);
        }
        for (String child : children) {
          zk.delete(path + "/" + child, -1);
        }
      }
    } catch (KeeperException.NoNodeException e) {} catch (KeeperException e) {
      throw new OrbZKFailure(e);
    } catch (InterruptedException e) {
      throw new OrbZKFailure(e);
    }
    
  }
  
  /**
   * 
   * @param zk
   *          - ZooKeeper
   * @param path
   *          - String
   * @param writable
   *          - Writable
   */
  public static void updateNodeData(ZooKeeper zk, String path, Writable writable) throws OrbZKFailure {
    try {
      zk.setData(path, writableToByteArray(writable), -1);
    } catch (KeeperException.NoNodeException e) {} catch (KeeperException e) {
      throw new OrbZKFailure(e);
    } catch (InterruptedException e) {
      throw new OrbZKFailure(e);
    } catch (IOException e) {
      throw new OrbZKFailure(e);
    }
  }
  
  /**
   * 
   * @param zk
   *          - ZooKeeper
   * @param path
   *          - String
   * @param writable
   *          - Writable
   */
  public static void existsUpdateNodeData(ZooKeeper zk, String path, Writable writable) throws OrbZKFailure {
    if (nodeExists(zk, path)) {
      updateNodeData(zk, path, writable);
    }
  }
  
  /**
   * Sets the data of an already existing node to the value of the writable.
   * 
   * @param zk
   *          is the zookeeper instance
   * @param path
   *          is the path of the node
   * @param writable
   *          is the Writable object who's data will set in the node
   * @throws OrbZKFailure
   */
  public static void setNodeData(ZooKeeper zk, String path, Writable writable) throws OrbZKFailure {
    try {
      zk.setData(path, writableToByteArray(writable), -1);
    } catch (KeeperException e) {
      e.printStackTrace();
      throw new OrbZKFailure(e);
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new OrbZKFailure(e);
    } catch (IOException e) {
      e.printStackTrace();
      throw new OrbZKFailure(e);
    }
    
  }
  
  /**
   * Return the children
   */
  public static List<String> getChildren(ZooKeeper zk, String path, Watcher watcher) throws OrbZKFailure {
    try {
      return zk.getChildren(path, watcher);
    } catch (KeeperException e) {
      throw new OrbZKFailure(e);
    } catch (InterruptedException e) {
      throw new OrbZKFailure(e);
    }
  }
  
}
