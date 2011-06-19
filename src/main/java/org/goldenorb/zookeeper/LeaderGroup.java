package org.goldenorb.zookeeper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
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
import org.goldenorb.event.MemberDataChangeEvent;
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
  private Class<? extends Member> memberClass;
  private ZooKeeper zk;
  private boolean processWatchedEvents = true;
  private volatile SortedMap<String,MEMBER_TYPE> members = new TreeMap<String,MEMBER_TYPE>();
  private SortedMap<String,MemberDataWatcher> watchers = new TreeMap<String,MemberDataWatcher>();
  private boolean fireEvents = false;
  
  public LeaderGroup(ZooKeeper zk,
                     OrbCallback orbCallback,
                     String path,
                     MEMBER_TYPE member,
                     Class<? extends Member> memberClass) {
    this.memberClass = memberClass;
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
      if (numOfMembers != 0) {
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
      MEMBER_TYPE memberW = null;
      for (String memberPath : memberList) {
        MemberDataWatcher watcher = null;
        if (watchers.containsKey(memberPath)) {
          memberW = (MEMBER_TYPE) ZookeeperUtils.getNodeWritable(zk, basePath + "/" + memberPath,
            memberClass, orbConf);
        } else { // set watcher for new node
          watcher = new MemberDataWatcher(memberPath);
          memberW = (MEMBER_TYPE) ZookeeperUtils.getNodeWritable(zk, basePath + "/" + memberPath,
            memberClass, orbConf, watcher);
          if (memberW != null) {
            watchers.put(memberPath, watcher);
          }
        }
        if (memberW != null) {
          members.put(memberPath, memberW);
        }
      }
      // check for watchers that need to be made inactive and removed
      Set<String> watcherSet = watchers.keySet();
      ArrayList<String> toRemove = new ArrayList<String>();
      if (watcherSet != null) {
        for (String memberPath : watcherSet) {
          if (!members.containsKey(memberPath)) {
            watchers.get(memberPath).makeInactive();
            toRemove.add(memberPath);
          }
        }
      }
      for (String memberPath : toRemove) {
        watchers.remove(memberPath);
      }
      
      if (numOfMembers > getNumOfMembers()) {
        fireEvent(new LostMemberEvent());
      } else if (numOfMembers < getNumOfMembers()) {
        fireEvent(new NewMemberEvent());
      }
      if (numOfMembers != 0 && getNumOfMembers() != 0) {
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
  
  public class MemberDataWatcher implements Watcher {
    
    private String nodePath;
    private String nodeName;
    private boolean active;
    
    public MemberDataWatcher(String nodeName) {
      this.nodePath = basePath + "/" + nodeName;
      this.nodeName = nodeName;
      // System.err.println("Watcher Created by:  for Node : " + nodePath);
      active = true;
    }
    
    public void makeInactive() {
      active = false;
    }
    
    @Override
    public void process(WatchedEvent event) {
      if ((event.getType() != Event.EventType.NodeDeleted) && LeaderGroup.this.isProcessWatchedEvents()
          && active) {
        try {
          MEMBER_TYPE node = (MEMBER_TYPE) ZookeeperUtils.getNodeWritable(zk, nodePath, memberClass, orbConf,
            this);
          if (event.getType() == Event.EventType.NodeDataChanged) {
            LeaderGroup.this.updateMembersData(nodeName, node);
          }
        } catch (OrbZKFailure e) {
          e.printStackTrace();
          LeaderGroup.this.fireEvent(new OrbExceptionEvent(e));
        }
      }
    }
  }
  
  public void updateMembersData(String memberPath, MEMBER_TYPE update) {
    if (update != null) {
      members.put(memberPath, update);
      fireEvent(new MemberDataChangeEvent());
    }
  }
  
  public Collection<MEMBER_TYPE> getMembers() {
    synchronized (members) {
      return members.values();
    }
  }
  
  public int getNumOfMembers() {
    synchronized (members) {
      return members.size();
    }
  }
  
  public boolean isLeader() {
    synchronized (members) {
      return member.equals(members.get(members.firstKey()));
    }
  }
  
  public MEMBER_TYPE getLeader() {
    synchronized (members) {
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
  
  public List<String> getMembersPath() {
    ArrayList<String> paths = new ArrayList<String>();
    Set<String> memberPaths = members.keySet();
    for (String memberPath : memberPaths) {
      paths.add(basePath + "/" + memberPath);
    }
    return paths;
  }
  
  public String getMyPath() {
    return myPath;
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
