/**
 * 
 */
package org.goldenorb.zookeeper.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.OrbTracker;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.jet.OrbTrackerMember;
import org.goldenorb.zookeeper.LeaderGroup;
import org.goldenorb.zookeeper.Member;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.junit.Test;


/**
 * @author rebanks
 *
 */
public class TestWatchMemberData {
  
  private static ZooKeeper zk;
  private static LeaderGroup<TMember> leaderGroup;
  private static String basePath = "/TestWatchMemberData"; 
  private static OrbConfiguration orbConf = new OrbConfiguration();
  private static TMember member = new TMember();
  
  @Test
  public void WatchMemberData() throws IOException, InterruptedException, OrbZKFailure, KeeperException {
    
    
    zk = ZookeeperUtils.connect("localhost");
    member.setData(1);
    ZookeeperUtils.tryToCreateNode(zk, basePath, CreateMode.PERSISTENT);
    OrbTracker ot = new OrbTracker(orbConf);
    leaderGroup = new LeaderGroup<TMember>(zk, ot.new OrbTrackerCallback(), basePath, member, (Class<? extends Member>) TMember.class);
    Collection<TMember> trackers = leaderGroup.getMembers();
    ArrayList<Integer> oldData = new ArrayList<Integer>();
    for(TMember orb : trackers) {
      oldData.add(orb.getData());
      System.out.println("one");
    }
    member.changeData(9999, zk, leaderGroup.getMyPath());
    ArrayList<Integer> newData = new ArrayList<Integer> ();
    List<String> children = zk.getChildren(basePath, false);
    
    trackers = leaderGroup.getMembers();
    
    
    for(TMember orb : trackers) {
      newData.add(orb.getData());
      System.out.println("two");
    }
    assertTrue(newData.get(0) == 9999);
    leaderGroup.leave();
    for(String node : children) {
      System.out.println("Child : "+basePath+"/"+node);
      ZookeeperUtils.deleteNodeIfEmpty(zk, basePath+"/"+node);
    }
    ZookeeperUtils.deleteNodeIfEmpty(zk, basePath);
    
    }
    
  }
  
  
 
 
