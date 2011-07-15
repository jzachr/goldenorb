package org.goldenorb.server;

import static org.junit.Assert.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.client.NodeDoesNotExistException;
import org.goldenorb.client.OrbTrackerMemberData;
import org.goldenorb.client.WatcherException;
import org.goldenorb.client.ZooKeeperConnectionException;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.jet.OrbTrackerMember;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestOrbTrackerMemberDataServiceImpl {
  
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
    leaderGroupPath = "/GoldenOrb/" + orbConf.getOrbClusterName() + "/OrbTrackerLeaderGroup";
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ZookeeperUtils.recursiveDelete(zk, "/GoldenOrb");
    ZookeeperUtils.deleteNodeIfEmpty(zk, "/GoldenOrb");
  }
  
  @After
  public void tearDown() throws Exception {
    ZookeeperUtils.recursiveDelete(zk, jobsInProgressPath);
    ZookeeperUtils.recursiveDelete(zk, jobQueuePath);
    ZookeeperUtils.recursiveDelete(zk, leaderGroupPath);
  }
  
  @Test
  public void testGetOrbTrackerMemberData() throws OrbZKFailure,
                                           ZooKeeperConnectionException,
                                           WatcherException,
                                           NodeDoesNotExistException,
                                           InterruptedException,
                                           KeeperException {
    int num_nodes = 10;
    OrbTrackerMember[] members = new OrbTrackerMember[num_nodes];
    for (int i = 0; i < num_nodes; i++) {
      members[i] = new OrbTrackerMember();
      members[i].setAvailablePartitions(i);
      members[i].setHostname("host");
      members[i].setInUsePartitions(i);
      members[i].setLeader(false);
      members[i].setPartitionCapacity(i);
      members[i].setPort(i);
      members[i].setReservedPartitions(i);
      ZookeeperUtils.tryToCreateNode(zk, leaderGroupPath + "/member", members[i],
        CreateMode.EPHEMERAL_SEQUENTIAL);
    }
    OrbTrackerMemberDataServiceImpl server = new OrbTrackerMemberDataServiceImpl();
    CountDownLatch jobs = new CountDownLatch(1);
    CountDownLatch update = new CountDownLatch(num_nodes);
    CountDownLatch remove = new CountDownLatch(3);
    server.enterTestingMode(jobs, remove, update);
    OrbTrackerMemberData[] memberData = server.getOrbTrackerMemberData();
    update.await();
    List<String> nodes = zk.getChildren(leaderGroupPath, false);
    assertEquals(nodes.size(), memberData.length);
    update = new CountDownLatch(2);
    server.setUpdateLatch(update);
    for (int i = 0; i < 2; i++) {
      members[i] = new OrbTrackerMember();
      members[i].setAvailablePartitions(i);
      members[i].setHostname("host");
      members[i].setInUsePartitions(i);
      members[i].setLeader(false);
      members[i].setPartitionCapacity(i);
      members[i].setPort(i);
      members[i].setReservedPartitions(i);
      ZookeeperUtils.tryToCreateNode(zk, leaderGroupPath + "/member", members[i],
        CreateMode.EPHEMERAL_SEQUENTIAL);
    }
    update.await();
    memberData = server.getOrbTrackerMemberData();
    nodes = zk.getChildren(leaderGroupPath, false);
    assertEquals(nodes.size(), memberData.length);
    remove = new CountDownLatch(4);
    server.setRemoveLatch(remove);
    int i = 0;
    for (String node : nodes) {
      if (i < 4) {
        ZookeeperUtils.deleteNodeIfEmpty(zk, leaderGroupPath + "/" + node);
      }
      i++;
    }
    remove.await();
    memberData = server.getOrbTrackerMemberData();
    nodes = zk.getChildren(leaderGroupPath, false);
    assertEquals(nodes.size(), memberData.length);
  }
  
  @Test
  public void testGetJobsInQueue() throws OrbZKFailure,
                                  NodeDoesNotExistException,
                                  ZooKeeperConnectionException,
                                  WatcherException,
                                  InterruptedException,
                                  KeeperException {
    OrbTrackerMemberDataServiceImpl server = new OrbTrackerMemberDataServiceImpl();
    int num_jobs = 10;
    CountDownLatch jobsLatch = new CountDownLatch(1);
    CountDownLatch updateLatch = new CountDownLatch(1);
    CountDownLatch removeLatch = new CountDownLatch(1);
    server.enterTestingMode(jobsLatch, removeLatch, updateLatch);
    for (int i = 0; i < num_jobs; i++) {
      ZookeeperUtils.tryToCreateNode(zk, jobQueuePath + "/job", CreateMode.EPHEMERAL_SEQUENTIAL);
    }
    String[] job = server.getJobsInQueue(); // initialize watcher
    jobsLatch.await();
    List<String> nodes = zk.getChildren(jobQueuePath, false);
    job = server.getJobsInQueue();
    assertEquals(nodes.size(), job.length);
    jobsLatch = new CountDownLatch(3);
    server.setJobsLatch(jobsLatch);
    for (int i = 0; i < 3; i++) {
      ZookeeperUtils.tryToCreateNode(zk, jobQueuePath + "/job", CreateMode.EPHEMERAL_SEQUENTIAL);
      Thread.sleep(5); // Need to make the number of watcher events deterministic
    }
    jobsLatch.await();
    nodes = zk.getChildren(jobQueuePath, false);
    job = server.getJobsInQueue();
    assertEquals(nodes.size(), job.length);
    jobsLatch = new CountDownLatch(5);
    server.setJobsLatch(jobsLatch);
    for (int i = 0; i < 5; i++) {
      ZookeeperUtils.deleteNodeIfEmpty(zk, jobQueuePath + "/" + nodes.get(i));
      Thread.sleep(5); // Need to make the number of watcher events deterministic
    }
    jobsLatch.await();
    nodes = zk.getChildren(jobQueuePath, false);
    job = server.getJobsInQueue();
    assertEquals(nodes.size(), job.length);
  }
  
  @Test
  public void testGetJobsInProgress() throws OrbZKFailure,
                                     NodeDoesNotExistException,
                                     ZooKeeperConnectionException,
                                     WatcherException,
                                     InterruptedException,
                                     KeeperException {
    OrbTrackerMemberDataServiceImpl server = new OrbTrackerMemberDataServiceImpl();
    int num_jobs = 10;
    CountDownLatch jobsLatch = new CountDownLatch(1);
    CountDownLatch updateLatch = new CountDownLatch(1);
    CountDownLatch removeLatch = new CountDownLatch(1);
    server.enterTestingMode(jobsLatch, removeLatch, updateLatch);
    for (int i = 0; i < num_jobs; i++) {
      ZookeeperUtils.tryToCreateNode(zk, jobsInProgressPath + "/job", CreateMode.EPHEMERAL_SEQUENTIAL);
    }
    String[] job = server.getJobsInProgress(); // initialize watcher
    jobsLatch.await();
    List<String> nodes = zk.getChildren(jobsInProgressPath, false);
    job = server.getJobsInProgress();
    assertEquals(nodes.size(), job.length);
    jobsLatch = new CountDownLatch(3);
    server.setJobsLatch(jobsLatch);
    for (int i = 0; i < 3; i++) {
      ZookeeperUtils.tryToCreateNode(zk, jobsInProgressPath + "/job", CreateMode.EPHEMERAL_SEQUENTIAL);
      Thread.sleep(5); // Need to make the number of watcher events deterministic
    }
    jobsLatch.await();
    nodes = zk.getChildren(jobsInProgressPath, false);
    job = server.getJobsInProgress();
    assertEquals(nodes.size(), job.length);
    jobsLatch = new CountDownLatch(5);
    server.setJobsLatch(jobsLatch);
    for (int i = 0; i < 5; i++) {
      ZookeeperUtils.deleteNodeIfEmpty(zk, jobsInProgressPath + "/" + nodes.get(i));
      Thread.sleep(5); // Need to make the number of watcher events deterministic
    }
    jobsLatch.await();
    nodes = zk.getChildren(jobsInProgressPath, false);
    job = server.getJobsInProgress();
    assertEquals(nodes.size(), job.length);
  }
  
  @Test
  public void testEveryThingChangingAtOnce() throws NodeDoesNotExistException,
                                            ZooKeeperConnectionException,
                                            WatcherException,
                                            InterruptedException,
                                            KeeperException {
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch allDone = new CountDownLatch(3);
    int cycles = 10;
    MemberThread[] threads = new MemberThread[3];
    for (int i = 0; i < 3; i++) {
      threads[i] = new MemberThread(i, cycles, start, allDone);
      threads[i].start();
    }
    OrbTrackerMemberDataServiceImpl server = new OrbTrackerMemberDataServiceImpl();
    // initialize watchers
    CountDownLatch notUsed = new CountDownLatch(1);
    server.enterTestingMode(notUsed, notUsed, notUsed);
    String[] jobQueue = server.getJobsInQueue();
    String[] jobsInProgress = server.getJobsInProgress();
    OrbTrackerMemberData[] members = server.getOrbTrackerMemberData();
    System.out.println("Start");
    start.countDown();
    allDone.await();
    List<String> mem = zk.getChildren(leaderGroupPath, false);
    List<String> jq = zk.getChildren(jobQueuePath, false);
    List<String> jip = zk.getChildren(jobsInProgressPath, false);
    jobQueue = server.getJobsInQueue();
    jobsInProgress = server.getJobsInProgress();
    members = server.getOrbTrackerMemberData();
    assertEquals(mem.size(), members.length);
    assertEquals(jq.size(), jobQueue.length);
    assertEquals(jip.size(), jobsInProgress.length);
  }
  
  public void createMember(int i) throws OrbZKFailure {
    OrbTrackerMember otm = new OrbTrackerMember();
    otm.setAvailablePartitions(i);
    otm.setHostname("host");
    otm.setInUsePartitions(i);
    otm.setLeader(false);
    otm.setPartitionCapacity(i);
    otm.setPort(i);
    otm.setReservedPartitions(i);
    ZookeeperUtils.tryToCreateNode(zk, leaderGroupPath, otm, CreateMode.EPHEMERAL_SEQUENTIAL);
  }
  
  public void createJob(String path) throws OrbZKFailure {
    ZookeeperUtils.tryToCreateNode(zk, path + "/job", CreateMode.EPHEMERAL_SEQUENTIAL);
  }
  
  public void deleteNode(String path) throws KeeperException, InterruptedException, OrbZKFailure {
    List<String> nodes = zk.getChildren(path, false);
    int num = (int) (nodes.size() * Math.random());
    int counter = 0;
    for (String node : nodes) {
      if (counter < num) {
        ZookeeperUtils.deleteNodeIfEmpty(zk, path + "/" + node);
      }
    }
  }
  
  public class MemberThread extends Thread {
    
    private int cycles;
    private CountDownLatch start;
    private CountDownLatch complete;
    private int type;
    
    public MemberThread(int type, int cycles, CountDownLatch start, CountDownLatch complete) {
      this.cycles = cycles;
      this.start = start;
      this.complete = complete;
      this.type = type;
    }
    
    public void run() {
      try {
        start.await();
        int num = 0;
        for (int i = 0; i < cycles; i++) {
          if (i % 2 == 0) {
            if (i == 0) {
              num = 10;
            } else {
              num = (int) (Math.random() * 10);
            }
            for (int j = 0; j < num; j++) {
              if (type == 0) {
                createMember(i);
              }
              if (type == 1) {
                createJob(jobsInProgressPath);
              } else {
                createJob(jobQueuePath);
              }
            }
          } else {
            if (type == 0) {
              deleteNode(leaderGroupPath);
            } else if (type == 1) {
              deleteNode(jobsInProgressPath);
            } else {
              deleteNode(jobQueuePath);
            }
          }
        }
        complete.countDown();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (OrbZKFailure e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (KeeperException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
}
