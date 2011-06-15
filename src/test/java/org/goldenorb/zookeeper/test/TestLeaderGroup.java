package org.goldenorb.zookeeper.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.junit.Test;

public class TestLeaderGroup {
  
  private final static int NUM_OF_MEMBERS = 10;
  List<Thread> threads = new ArrayList<Thread>();
  List<TTracker> trackers = new ArrayList<TTracker>();
  
  
  @Test
  public void leaderGroupTest() throws IOException, InterruptedException {

    ZooKeeper zk = ZookeeperUtils.connect("localhost");
    String basePath = "/" + "Job";
    CountDownLatch startCountDownLatch = new CountDownLatch(NUM_OF_MEMBERS);
    CountDownLatch leaderChangeCdl = new CountDownLatch(NUM_OF_MEMBERS -1);
    CountDownLatch leaveCdl = new CountDownLatch(NUM_OF_MEMBERS);

    for (int i = 0; i < NUM_OF_MEMBERS; i++) {
      TTracker tracker = new TTracker(zk, i,  basePath, startCountDownLatch, leaderChangeCdl, leaveCdl);
      trackers.add(tracker);
      threads.add(new Thread(tracker));
      threads.get(i).start();
    }
    startCountDownLatch.await();
    int numOfLeaders = 0;
    int leader = -1;
    for(int i=0; i < NUM_OF_MEMBERS; i++){
      if(trackers.get(i).isLeader()){
        leader = i;
        numOfLeaders++;
      }
    }
    assertTrue(numOfLeaders == 1);
    trackers.get(leader).leave();
    leaderChangeCdl.await();
    numOfLeaders = 0;
    for(int i=0; i < NUM_OF_MEMBERS; i++){
      if(i != leader){
        if(trackers.get(i).isLeader()){
          numOfLeaders++;
        }
      }
    }
    assertTrue(numOfLeaders == 1);
    for(int i=0; i < NUM_OF_MEMBERS; i++){
      if(i != leader){
        trackers.get(i).leave();
      }
    }
    leaveCdl.await();
  }
}
