package org.goldenorb.server;

import static org.junit.Assert.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJobsMonitor {
  
  private static ZooKeeper zk;
  private static String jobQueuePath;
  private static String jobsInProgressPath;
  private static String leaderGroupPath;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    OrbConfiguration orbConf = new OrbConfiguration(true);
    orbConf.setOrbZooKeeperQuorum("localhost:21810");
    zk = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
    ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb");
    ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb/" + orbConf.getOrbClusterName());
    ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobsInProgress");
    ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobQueue");
    ZookeeperUtils
        .tryToCreateNode(zk, "/GoldenOrb/" + orbConf.getOrbClusterName() + "/OrbTrackerLeaderGroup");
    jobsInProgressPath = "/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobsInProgress";
    jobQueuePath = "/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobQueue";
    leaderGroupPath = orbConf.getOrbClusterName() + "/OrbTrackerLeaderGroup";
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ZookeeperUtils.recursiveDelete(zk, "/GoldenOrb");
    ZookeeperUtils.deleteNodeIfEmpty(zk, "/GoldenOrb");
  }
  
  @After
  public void tearDown() throws Exception {
    ZookeeperUtils.recursiveDelete(zk, "/GoldenOrb/Test/JobsInProgress");
    ZookeeperUtils.recursiveDelete(zk, "/GoldenOrb/Test/JobQueue");
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testJobQueue() throws OrbZKFailure, InterruptedException, KeeperException {
    int num_nodes = 10;
    for (int i = 0; i < num_nodes; i++) {
      createJob(jobQueuePath);
    }
    CountDownLatch latch = new CountDownLatch(1);
    TServer server = new TServer(latch);
    String[] jobQueue = server.getJobsInQueue();
    assertEquals(null, jobQueue);
    new JobsMonitor(jobQueuePath, server, zk);
    latch.await();
    jobQueue = server.getJobsInQueue();
    assertEquals(num_nodes, jobQueue.length);
    latch = new CountDownLatch(3);
    server.setLatcher(latch);
    for (int i = 0; i < 3; i++) {
      createJob(jobQueuePath);
    }
    latch.await();
    jobQueue = server.getJobsInQueue();
    assertEquals(num_nodes + 3, jobQueue.length);
    latch = new CountDownLatch(3);
    server.setLatcher(latch);
    List<String> children = zk.getChildren(jobQueuePath, false);
    for (int i = 0; i < 3; i++) {
      ZookeeperUtils.deleteNodeIfEmpty(zk, jobQueuePath + "/" + children.get(3 - i));
    }
    latch.await();
    jobQueue = server.getJobsInQueue();
    assertEquals(num_nodes, jobQueue.length);
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testJobsInProgress() throws OrbZKFailure, InterruptedException, KeeperException {
    int num_nodes = 10;
    for (int i = 0; i < num_nodes; i++) {
      createJob(jobsInProgressPath);
    }
    CountDownLatch latch = new CountDownLatch(1);
    TServer server = new TServer(latch);
    String[] jobQueue = server.getJobsInProgress();
    assertEquals(null, jobQueue);
    new JobsMonitor(jobsInProgressPath, server, zk);
    latch.await();
    jobQueue = server.getJobsInProgress();
    assertEquals(num_nodes, jobQueue.length);
    latch = new CountDownLatch(3);
    server.setLatcher(latch);
    for (int i = 0; i < 3; i++) {
      createJob(jobsInProgressPath);
    }
    latch.await();
    jobQueue = server.getJobsInProgress();
    assertEquals(num_nodes + 3, jobQueue.length);
    latch = new CountDownLatch(3);
    server.setLatcher(latch);
    List<String> children = zk.getChildren(jobsInProgressPath, false);
    for (int i = 0; i < 3; i++) {
      ZookeeperUtils.deleteNodeIfEmpty(zk, jobsInProgressPath + "/" + children.get(3 - i));
    }
    latch.await();
    jobQueue = server.getJobsInProgress();
    assertEquals(num_nodes, jobQueue.length);
  }
  
  private void createJob(String path) throws OrbZKFailure {
    ZookeeperUtils.tryToCreateNode(zk, path + "/Job", CreateMode.EPHEMERAL_SEQUENTIAL);
  }
  
}
