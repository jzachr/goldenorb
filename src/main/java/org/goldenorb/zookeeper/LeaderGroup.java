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
import org.mortbay.log.Log;

public class LeaderGroup<MEMBER_TYPE extends Member> implements OrbConfigurable {
  
  private OrbConfiguration orbConf;
  private OrbCallback orbCallback;
  private String basePath;
  private String myPath;
  private MEMBER_TYPE member;
  private ZooKeeper zk;
  private boolean processWatchedEvents = true;
  private SortedMap<String,MEMBER_TYPE> members = new TreeMap<String,MEMBER_TYPE>();
  private boolean fireEvents = false;
  
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
      fireEvents = true;
    } catch (OrbZKFailure e) {
      fireEvent(new OrbExceptionEvent(e));
    }
  }
  
  @SuppressWarnings("unchecked")
  public void updateMembers() throws OrbZKFailure {
    synchronized (members) {
      int numOfMembers = getNumOfMembers();
      MEMBER_TYPE leaderMember;
      if (numOfMembers != 0){
        leaderMember = getLeader();
      } else {
        leaderMember = null;
      }
      List<String> memberList;
      try {
        memberList = zk.getChildren(basePath, new WatchMembers());
      } catch (KeeperException e) {
        e.printStackTrace();
        throw new OrbZKFailure(e);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new OrbZKFailure(e);
      }
      members.clear();
      for (String memberPath : memberList) {
        MEMBER_TYPE memberW = (MEMBER_TYPE) ZookeeperUtils.getNodeWritable(zk, basePath + "/" + memberPath,
          member.getClass(), orbConf);
        if (memberW != null) {
          members.put(memberPath, memberW);
        }
      }
      if (numOfMembers > getNumOfMembers()) {
        fireEvent(new LostMemberEvent());
      } else if (numOfMembers < getNumOfMembers()) {
        fireEvent(new NewMemberEvent());
      }
      if (numOfMembers != 0 && getNumOfMembers() != 0){
        if (!leaderMember.equals(getLeader())) {
          fireEvent(new LeadershipChangeEvent());
        }
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
    synchronized(members){
      return members.values();
    }
  }
  
  public int getNumOfMembers() {
    synchronized(members){
      return members.size();
    }
  }
  
  public boolean isLeader() {
    synchronized(members){
      return member.equals(members.get(members.firstKey()));
    }
  }
  
  public MEMBER_TYPE getLeader() {
    synchronized(members){
      return members.get(members.firstKey());
    }
  }
  
  public void fireEvent(OrbEvent e) {
    if (fireEvents) {
      orbCallback.process(e);
    }
  }
  
  public void setOrbConf(OrbConfiguration orbConf) {
    this.orbConf = orbConf;
  }
  
  public OrbConfiguration getOrbConf() {
    return orbConf;
  }
  
  public void leave() {
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
