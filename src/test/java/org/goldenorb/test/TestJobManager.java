package org.goldenorb.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.LongWritable;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.OrbRunner;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.goldenorb.zookeeper.test.TJTracker;
import org.junit.Test;

public class TestJobManager extends OrbRunner{
  private final static int NUM_OF_MEMBERS = 10;
  private final static int HEARTBEAT_INTERVAL = 500; //in ms
  List<Thread> threads = new ArrayList<Thread>();
  List<TJTracker> trackers = new ArrayList<TJTracker>();
  private ZooKeeper zk;
  
  @Test
  public void jobManagerTest() throws IOException, InterruptedException, OrbZKFailure {
    zk = ZookeeperUtils.connect("localhost");
    ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb");
    ZookeeperUtils.tryToCreateNode(zk, "/GoldenOrb/OrbCluster");    
    CountDownLatch joinLeaderGroup = new CountDownLatch(NUM_OF_MEMBERS); //everyone join?
    CountDownLatch exit = new CountDownLatch(NUM_OF_MEMBERS); //everyone ready to exit?
    CountDownLatch jobCreated = new CountDownLatch(1);
    CountDownLatch newLeader = new CountDownLatch(1);
    OrbConfiguration orbConf = new OrbConfiguration();
    orbConf.setOrbClusterName("OrbCluster");
    orbConf.setJobHeartbeatTimeout(1000);
    orbConf.setMaximumJobTries(3);
    System.out.println(orbConf.getJobHeartbeatTimeout());
    // Create all of the test Trackers
    for (int i = 0; i < NUM_OF_MEMBERS; i++) {
      TJTracker tracker = new TJTracker(zk, joinLeaderGroup, exit, orbConf, i, "/GoldenOrb/OrbCluster");
      trackers.add(tracker);
      threads.add(new Thread(tracker));
      threads.get(i).start();
    }
    joinLeaderGroup.await();
    orbConf.setOrbZooKeeperQuorum("localhost");
    
    String path1 = runJob(orbConf);
    String path2 = runJob(orbConf);
    
    new Thread(new HeartbeatUpdater(getJobInProgressPath(path1))).start();
    new Thread(new HeartbeatUpdater(getJobInProgressPath(path2))).start();
    jobCreated.await(2, TimeUnit.SECONDS);
    int leader = 0;
    for (int i = 0; i < NUM_OF_MEMBERS; i++){
      if(trackers.get(i).isLeader()){
        leader = i;
      }
    }
    trackers.get(leader).leave();
    
    newLeader.await(5, TimeUnit.SECONDS);

    //exit and shutdown
    for (int i = 0; i < NUM_OF_MEMBERS; i++) {
      trackers.get(i).leave();
    }
    exit.await();
    ZookeeperUtils.recursiveDelete(zk, "/GoldenOrb");
    ZookeeperUtils.deleteNodeIfEmpty(zk, "/GoldenOrb");
  }
  
  private class HeartbeatUpdater implements Runnable{
    
    private String pathToUpdate;
    private boolean active = true;
    
    private Long heartbeat = 1L;
    
    public HeartbeatUpdater(String pathToUpdate){
      this.pathToUpdate = pathToUpdate;
    }
    @Override
    public void run() {
      while(active){
        synchronized(this){
          try {
            wait(HEARTBEAT_INTERVAL);
            try {
              ZookeeperUtils.existsUpdateNodeData(zk, pathToUpdate + "/messages/heartbeat", new LongWritable(heartbeat++));
              System.err.println("Creating heartbeat for: " + pathToUpdate + "/messages/heartbeat" + " heartbeat is: " + heartbeat);
            } catch (OrbZKFailure e) {
              e.printStackTrace();
            }
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }
    
    public void kill(){
      active = false;
    }
  }
  
  private static String getJobInProgressPath(String path){
    String[] pieces = path.split("/");
    String importantPiece = pieces[pieces.length -1];
    return "/GoldenOrb/OrbCluster/JobsInProgress/" + importantPiece;
  }
}
