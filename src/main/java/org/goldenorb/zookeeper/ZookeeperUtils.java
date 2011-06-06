package org.goldenorb.zookeeper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.LoggerFactory;

public class ZookeeperUtils {
  
  private static ConnectWatcher connect;
  private static Logger LOG = LoggerFactory.getLogger(ZookeeperUtils.class);
  
  public static ZooKeeper connect(String hosts) throws IOException, InterruptedException {
    if (connect == null) {
      connect = new ConnectWatcher();
    }
    return connect.connect(hosts);
  }
  
  public static byte[] writableToByteArray(Writable w) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    w.write(out);
    return baos.toByteArray();
  }
  
  public static Writable ByteArrayToWritable(byte[] byteArray, Class<Writable> writableClass) throws InstantiationException,
                                                                                             IllegalAccessException,
                                                                                             IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
    DataInput in = new DataInputStream(bais);
    
    Writable w = (Writable) writableClass.newInstance();
    w.readFields(in);
    return w;
  }
  
  public static Writable ByteArrayToWritable(byte[] byteArray, Writable w) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
    DataInput in = new DataInputStream(bais);
    
    w.readFields(in);
    return w;
  }
  
  public static boolean nodeExists(ZooKeeper zk, String string) {
    Stat stat;
    try {
      stat = zk.exists(string, false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return stat != null;
  }
  
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
  
  public static String tryToCreateNode(ZooKeeper zk, String path, Writable node, CreateMode createMode) throws OrbZKFailure {
    String result = null;
    try {
      result = zk.create(path, writableToByteArray(node), Ids.OPEN_ACL_UNSAFE, createMode);
    } catch (KeeperException.NodeExistsException e) {
      LOG.debug("Node " + path + " already exists!");
    } catch (KeeperException e) {
      throw new OrbZKFailure(e);
    } catch (InterruptedException e) {
      throw new OrbZKFailure(e);
    } catch (IOException e) {
      throw new OrbZKFailure(e);
    }
    return result;
  }
  
  public static void notExistCreateNode(ZooKeeper zk, String path) throws OrbZKFailure {
    if (!nodeExists(zk, path)) {
      tryToCreateNode(zk, path);
    }
  }
  
  public static void notExistCreateNode(ZooKeeper zk, String path, CreateMode createMode) throws OrbZKFailure {
    if (!nodeExists(zk, path)) {
      tryToCreateNode(zk, path, createMode);
    }
  }
  
}
