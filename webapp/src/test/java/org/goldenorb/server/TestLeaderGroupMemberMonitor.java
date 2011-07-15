package org.goldenorb.server;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.client.OrbTrackerMemberData;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.jet.OrbTrackerMember;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestLeaderGroupMemberMonitor {
  
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
  
  /**
   * Remove nodes used for testing
   * 
   * @throws Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ZookeeperUtils.recursiveDelete(zk, "/GolenOrb");
  }
  
  /**
   * Test a single change to a member node
   * 
   * @throws OrbZKFailure
   * @throws InterruptedException
   */
  @Test
  public void testSingleMemberDataChange() throws OrbZKFailure, InterruptedException {
    CountDownLatch cdlatch = new CountDownLatch(1);
    TServer server = new TServer(cdlatch);
    OrbTrackerMember otm = new OrbTrackerMember();
    otm.setAvailablePartitions(1);
    otm.setHostname("TEST");
    otm.setInUsePartitions(1);
    otm.setLeader(true);
    otm.setPartitionCapacity(1);
    otm.setPort(1);
    otm.setReservedPartitions(1);
    String path = ZookeeperUtils.tryToCreateNode(zk, leaderGroupPath + "/member", otm,
      CreateMode.EPHEMERAL_SEQUENTIAL);
    new LeaderGroupMemberMonitor(path, zk, path.split("/")[3], server);
    cdlatch.await();
    Map<String,OrbTrackerMemberData> map = server.getMemberData();
    assertTrue(map.size() == 1);
    Set<String> keys = map.keySet();
    assertTrue(keys.size() == 1);
    String nodeName = "";
    for (String key : keys) {
      nodeName = key;
    }
    assertTrue(nodeName.equals(path.split("/")[3]));
    assertTrue(map.get(nodeName).getAvailablePartitions() == otm.getAvailablePartitions());
    assertTrue(map.get(nodeName).getPort() == otm.getPort());
    otm.setAvailablePartitions(9);
    cdlatch = new CountDownLatch(1);
    server.setLatcher(cdlatch);
    ZookeeperUtils.setNodeData(zk, path, otm);
    cdlatch.await();
    map = server.getMemberData();
    assertTrue(map.size() == 1);
    keys = map.keySet();
    assertTrue(keys.size() == 1);
    nodeName = "";
    for (String key : keys) {
      nodeName = key;
    }
    assertTrue(nodeName.equals(path.split("/")[3]));
    assertTrue(map.get(nodeName).getAvailablePartitions() == otm.getAvailablePartitions());
  }
  
  /**
   * Test multiple changes to a single member node
   * 
   * @throws OrbZKFailure
   * @throws InterruptedException
   */
  public void testMulitpleChanges() throws OrbZKFailure, InterruptedException {
    CountDownLatch cdlatch = new CountDownLatch(1);
    TServer server = new TServer(cdlatch);
    OrbTrackerMember otm = new OrbTrackerMember();
    otm.setAvailablePartitions(1);
    otm.setHostname("TEST");
    otm.setInUsePartitions(1);
    otm.setLeader(true);
    otm.setPartitionCapacity(1);
    otm.setPort(1);
    otm.setReservedPartitions(1);
    String path = ZookeeperUtils.tryToCreateNode(zk, leaderGroupPath + "/member", otm,
      CreateMode.EPHEMERAL_SEQUENTIAL);
    new LeaderGroupMemberMonitor(path, zk, path.split("/")[3], server);
    cdlatch.await();
    Map<String,OrbTrackerMemberData> map = server.getMemberData();
    assertTrue(map.size() == 1);
    Set<String> keys = map.keySet();
    assertTrue(keys.size() == 1);
    String nodeName = "";
    for (String key : keys) {
      nodeName = key;
    }
    assertTrue(nodeName.equals(path.split("/")[3]));
    assertTrue(map.get(nodeName).getAvailablePartitions() == otm.getAvailablePartitions());
    assertTrue(map.get(nodeName).getPort() == otm.getPort());
    otm.setAvailablePartitions(9);
    cdlatch = new CountDownLatch(1);
    server.setLatcher(cdlatch);
    ZookeeperUtils.setNodeData(zk, path, otm);
    cdlatch.await();
    map = server.getMemberData();
    assertTrue(map.size() == 1);
    keys = map.keySet();
    assertTrue(keys.size() == 1);
    nodeName = "";
    for (String key : keys) {
      nodeName = key;
    }
    assertTrue(nodeName.equals(path.split("/")[3]));
    assertTrue(map.get(nodeName).getAvailablePartitions() == otm.getAvailablePartitions());
    cdlatch = new CountDownLatch(1);
    server.setLatcher(cdlatch);
    otm.setHostname("Hostname");
    ZookeeperUtils.setNodeData(zk, path, otm);
    cdlatch.await();
    map = server.getMemberData();
    assertTrue(map.size() == 1);
    keys = map.keySet();
    assertTrue(keys.size() == 1);
    nodeName = "";
    for (String key : keys) {
      nodeName = key;
    }
    assertTrue(nodeName.equals(path.split("/")[3]));
    assertTrue(map.get(nodeName).getHostname() == otm.getHostname());
    cdlatch = new CountDownLatch(1);
    server.setLatcher(cdlatch);
    otm.setHostname("Host");
    ZookeeperUtils.setNodeData(zk, path, otm);
    cdlatch.await();
    map = server.getMemberData();
    assertTrue(map.size() == 1);
    keys = map.keySet();
    assertTrue(keys.size() == 1);
    nodeName = "";
    for (String key : keys) {
      nodeName = key;
    }
    assertTrue(nodeName.equals(path.split("/")[3]));
    assertTrue(map.get(nodeName).getHostname() == otm.getHostname());
  }
  
}
