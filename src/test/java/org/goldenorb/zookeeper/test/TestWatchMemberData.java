/**
 * 
 */
package org.goldenorb.zookeeper.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.junit.Test;



public class TestWatchMemberData {
  
  private static ZooKeeper zk;
  private static String basePath = "/TestWatchMemberData"; 
  private static TMember member = new TMember();
  
/**
 * 
 */
  @Test
  public void WatchMemberData() throws IOException, InterruptedException, OrbZKFailure, KeeperException {
    
    
    zk = ZookeeperUtils.connect("localhost");
    int data = 1;
    member.setData(data);
    ZookeeperUtils.tryToCreateNode(zk, basePath, CreateMode.PERSISTENT);
    CountDownLatch dataChangedCdl = new CountDownLatch(1);
    CountDownLatch startCdl = new CountDownLatch(1);
    CountDownLatch leaderChangeCdl = new CountDownLatch(1);
    CountDownLatch leaveCdl = new CountDownLatch(1);
    
    TTracker tt = new TTracker(zk, data, basePath, startCdl,leaderChangeCdl, leaveCdl, dataChangedCdl);
    tt.run();
    startCdl.await();
    
    int newData = 9999;
    tt.changeMemberData(newData);
    dataChangedCdl.await();
    assertTrue(tt.getMemberData() == newData);
    
    tt.leave();
    List<String> children = zk.getChildren(basePath, false);
    for(String node : children) {
      ZookeeperUtils.deleteNodeIfEmpty(zk, basePath+"/"+node);
    }
    ZookeeperUtils.deleteNodeIfEmpty(zk, basePath);
    
    }
    
  }
  
  
 
 
