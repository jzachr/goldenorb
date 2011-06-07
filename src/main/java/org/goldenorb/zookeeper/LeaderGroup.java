package org.goldenorb.zookeeper;

import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.event.LeadershipChangeEvent;
import org.goldenorb.event.LostMemberEvent;
import org.goldenorb.event.NewMemberEvent;
import org.goldenorb.event.OrbCallback;
import org.goldenorb.event.OrbEvent;
import org.goldenorb.event.OrbExceptionEvent;

public class LeaderGroup<MEMBER_TYPE extends Member> implements OrbConfigurable {
  
  private OrbConfiguration orbConf;
  private OrbCallback orbCallback;
  private String basePath;
  private String myPath;
  private MEMBER_TYPE member;
  private ZooKeeper zk;
  private boolean processWatchedEvents;
  private SortedMap<String,MEMBER_TYPE> members = new TreeMap<String,MEMBER_TYPE>();
  
  public LeaderGroup(ZooKeeper zk, OrbCallback orbCallback, String path, MEMBER_TYPE member) {
    this.orbCallback = orbCallback;
    this.basePath = path;
    this.member = member;
    this.zk = zk;
    init();
  }
  
  public void init() {
    try {
      ZookeeperUtils.notExistCreateNode(zk, basePath);
      myPath = ZookeeperUtils.tryToCreateNode(zk, basePath + "/member", member,
        CreateMode.EPHEMERAL_SEQUENTIAL);
      members.put(myPath, member);
      updateMembers();
    } catch (OrbZKFailure e) {
      fireEvent(new OrbExceptionEvent(e));
    }
  }
  
  @SuppressWarnings("unchecked")
  public void updateMembers() throws OrbZKFailure {
    synchronized (members) {
      int numOfMembers = getNumOfMembers();
      MEMBER_TYPE leaderMember = getLeader();
      
      List<String> memberList;
      try {
        memberList = zk.getChildren(basePath, new WatchMembers());
      } catch (KeeperException e) {
        throw new OrbZKFailure(e);
      } catch (InterruptedException e) {
        throw new OrbZKFailure(e);
      }
      
      for (String memberPath : memberList) {
        MEMBER_TYPE memberW = (MEMBER_TYPE) ZookeeperUtils.getNodeWritable(zk, memberPath, member.getClass(),
          orbConf);
        if (memberW != null) {
          members.put(memberPath, memberW);
        }
      }
      if (numOfMembers > getNumOfMembers()) {
        fireEvent(new LostMemberEvent());
      } else if (numOfMembers < getNumOfMembers()) {
        fireEvent(new NewMemberEvent());
      }
      if (!leaderMember.equals(getLeader())) {
        fireEvent(new LeadershipChangeEvent());
      }
    }
  }
  
  public class WatchMembers implements Watcher {
    
    public void process(WatchedEvent event) {
      if (LeaderGroup.this.isProcessWatchedEvents()) {
        try {
          updateMembers();
        } catch (OrbZKFailure e) {
          fireEvent(new OrbExceptionEvent(e));
        }
      }
    }
  }
  
  public Collection<MEMBER_TYPE> getMembers() {
    return members.values();
  }
  
  public int getNumOfMembers() {
    return members.size();
  }
  
  public boolean isLeader() {
    return member == members.get(members.firstKey());
  }
  
  public MEMBER_TYPE getLeader() {
    return members.get(members.firstKey());
  }
  
  public void fireEvent(OrbEvent e) {
    orbCallback.process(e);
  }
  
  public void setOrbConf(OrbConfiguration orbConf) {
    this.orbConf = orbConf;
  }
  
  public OrbConfiguration getOrbConf() {
    return orbConf;
  }
  
  public void leave(){
    this.processWatchedEvents = false;
    try {
      ZookeeperUtils.deleteNodeIfEmpty(zk, myPath);
    } catch (OrbZKFailure e) {
      fireEvent(new OrbExceptionEvent(e));
    }
  }
  
  protected boolean isProcessWatchedEvents() {
    return processWatchedEvents;
  }
  
  protected void setProcessWatchedEvents(boolean processWatchedEvents) {
    this.processWatchedEvents = processWatchedEvents;
  }
}
