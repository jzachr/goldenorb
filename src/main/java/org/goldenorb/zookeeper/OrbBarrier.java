package org.goldenorb.zookeeper;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;

public class OrbBarrier implements Barrier {
  
  private OrbConfiguration orbConf;
  String barrierName;
  int numOfMembers;
  String member;
  ZooKeeper zk;
  
  public OrbBarrier(OrbConfiguration orbConf,
                        String barrierName,
                        int numOfMembers,
                        String member,
                        ZooKeeper zk) {
    this.orbConf = orbConf;
    this.barrierName = barrierName;
    this.numOfMembers = numOfMembers;
    this.member = member;
    this.zk = zk;
  }
  
  @Override
  public void enter() throws OrbZKFailure {
    // general path looks like: "/barrierName/member"
    String barrierPath = "/" + barrierName;
    String memberPath = barrierPath + "/" + member;
    
    ZookeeperUtils.tryToCreateNode(zk, barrierPath, CreateMode.PERSISTENT);
    ZookeeperUtils.tryToCreateNode(zk, memberPath, CreateMode.EPHEMERAL);
    
    try {
      BarrierWait bw = new BarrierWait(this);
      List<String> list = zk.getChildren(barrierPath, bw);
      
      // O(N^2) implementation
      while (list.size() < numOfMembers) {
        synchronized (this) {
          wait(2000);
          list = zk.getChildren(barrierPath, bw);
        }
      }
      
    } catch (InterruptedException e) {
      throw new OrbZKFailure(e);
    } catch (KeeperException e) {
      throw new OrbZKFailure(e);
    }
    
  }
  
  @Override
  public void setOrbConf(OrbConfiguration orbConf) {
    this.orbConf = orbConf;
  }
  
  @Override
  public OrbConfiguration getOrbConf() {
    return orbConf;
  }
  
  class BarrierWait implements Watcher {
    OrbBarrier ob;
    
    public BarrierWait(OrbBarrier ob) {
      this.ob = ob;
    }
    
    @Override
    public void process(WatchedEvent event) {
      synchronized (ob) {
        ob.notify();
      }
    }
    
  }
}
