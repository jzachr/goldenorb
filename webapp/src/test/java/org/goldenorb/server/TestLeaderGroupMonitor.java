package org.goldenorb.server;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.client.OrbTrackerMemberData;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.jet.OrbTrackerMember;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLeaderGroupMonitor {
  private static ZooKeeper zk;
  private static String leaderGroupPath;
  
  /**
   * Sets up zookeeper and nodes for testing.
   * 
   * @throws Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    OrbConfiguration orbConf = new OrbConfiguration(true);
    orbConf.setOrbZooKeeperQuorum("localhost:21810");
    zk = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb");
    ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb/Test");
    ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb/Test/OrbTrackerLeaderGroup");
    leaderGroupPath = "/GoldenOrb/Test/OrbTrackerLeaderGroup";
  }
  
  @After
  public void cleanUpNodes() throws OrbZKFailure {
    ZookeeperUtils.recursiveDelete(zk, leaderGroupPath);
  }
  
  /**
   * Remove nodes used for testing
   * 
   * @throws Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ZookeeperUtils.recursiveDelete(zk, "/GolenOrb");
    ZookeeperUtils.deleteNodeIfEmpty(zk, "/GoldenOrb");
  }
  
  /**
   * Test initializing a monitor on a leader group that already contains members
   * 
   * @throws OrbZKFailure
   * @throws InterruptedException
   */
  @Test
  public void testInitializing() throws OrbZKFailure, InterruptedException {
    int num_nodes = 10;
    for (int i = 0; i < num_nodes; i++) {
      OrbTrackerMember otm = new OrbTrackerMember();
      otm.setAvailablePartitions(1);
      otm.setHostname("abc");
      otm.setInUsePartitions(1);
      otm.setLeader(false);
      otm.setPartitionCapacity(1);
      otm.setPort(1);
      otm.setReservedPartitions(1);
      ZookeeperUtils.tryToCreateNode(zk, leaderGroupPath + "/member", otm, CreateMode.EPHEMERAL_SEQUENTIAL);
    }
    CountDownLatch latch = new CountDownLatch(num_nodes);
    TServer server = new TServer(latch);
    Map<String,OrbTrackerMemberData> map = server.getMemberData();
    assertTrue(map.size() == 0);
    new LeaderGroupMonitor(leaderGroupPath, server, zk);
    latch.await();
    map = server.getMemberData();
    assertTrue(map.size() == num_nodes);
    // ZookeeperUtils.recursiveDelete(zk, leaderGroupPath);
  }
  
  /**
   * Test adding nodes to alreading initialized watcher
   * 
   * @throws OrbZKFailure
   * @throws InterruptedException
   */
  @Test
  public void testAddingNodes() throws OrbZKFailure, InterruptedException {
    int num_nodes = 10;
    CountDownLatch latch = new CountDownLatch(num_nodes);
    TServer server = new TServer(latch);
    Map<String,OrbTrackerMemberData> map = server.getMemberData();
    assertTrue(map.size() == 0);
    new LeaderGroupMonitor(leaderGroupPath, server, zk);
    for (int i = 0; i < num_nodes; i++) {
      OrbTrackerMember otm = new OrbTrackerMember();
      otm.setAvailablePartitions(1);
      otm.setHostname("abc");
      otm.setInUsePartitions(1);
      otm.setLeader(false);
      otm.setPartitionCapacity(1);
      otm.setPort(1);
      otm.setReservedPartitions(1);
      ZookeeperUtils.tryToCreateNode(zk, leaderGroupPath + "/member", otm, CreateMode.EPHEMERAL_SEQUENTIAL);
    }
    latch.await();
    map = server.getMemberData();
    assertEquals(num_nodes, map.size());
    latch = new CountDownLatch(5); // adding five new nodes
    server.setLatcher(latch);
    for (int i = 0; i < 5; i++) {
      OrbTrackerMember otm = new OrbTrackerMember();
      otm.setAvailablePartitions(1);
      otm.setHostname("abc");
      otm.setInUsePartitions(1);
      otm.setLeader(false);
      otm.setPartitionCapacity(1);
      otm.setPort(1);
      otm.setReservedPartitions(1);
      ZookeeperUtils.tryToCreateNode(zk, leaderGroupPath + "/member", otm, CreateMode.EPHEMERAL_SEQUENTIAL);
    }
    latch.await();
    map = server.getMemberData();
    assertTrue(map.size() == num_nodes + 5);
  }
  
  @Test
  public void testDeleteAndAdd() throws OrbZKFailure, InterruptedException, KeeperException {
    int num_created = 10;
    int num_deleted = 7;
    CountDownLatch latch = new CountDownLatch(num_created);
    TServer server = new TServer(latch);
    Map<String,OrbTrackerMemberData> map = server.getMemberData();
    assertTrue(map.size() == 0);
    new LeaderGroupMonitor(leaderGroupPath, server, zk);
    for (int i = 0; i < num_created; i++) {
      OrbTrackerMember otm = new OrbTrackerMember();
      otm.setAvailablePartitions(1);
      otm.setHostname("abc");
      otm.setInUsePartitions(1);
      otm.setLeader(false);
      otm.setPartitionCapacity(1);
      otm.setPort(1);
      otm.setReservedPartitions(1);
      ZookeeperUtils.tryToCreateNode(zk, leaderGroupPath + "/member", otm, CreateMode.EPHEMERAL_SEQUENTIAL);
    }
    latch.await();
    map = server.getMemberData();
    assertTrue(map.size() == num_created);
    latch = new CountDownLatch(num_deleted);
    server.setLatcher(latch);
    List<String> children = zk.getChildren(leaderGroupPath, false);
    int i = 0;
    for (String child : children) {
      if (i < num_deleted) {
        ZookeeperUtils.deleteNodeIfEmpty(zk, leaderGroupPath + "/" + child);
      }
      i++;
    }
    latch.await();
    map = server.getMemberData();
    assertEquals(num_created - num_deleted, map.size());
  }
  
}
