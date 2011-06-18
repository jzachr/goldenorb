package org.goldenorb.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.event.OrbExceptionEvent;

public class WatchMemberData implements Watcher {
  
  private LeaderGroup leaderGroup;
  private String path;
  private String memberPath;
  private Class<? extends Member> memberClass;
  private ZooKeeper zk;
  private OrbConfiguration orbConf;
  
  public WatchMemberData (LeaderGroup leaderGroup,
                          String basePath,
                          String memberPath,
                          Class<? extends Member> memberClass,
                          ZooKeeper zk,
                          OrbConfiguration orbConf) throws OrbZKFailure {
    this.leaderGroup = leaderGroup;
    this.path = basePath + "/" + memberPath;
    this.memberPath = memberPath;
    this.memberClass = memberClass;
    this.zk = zk;
    this.orbConf = orbConf;
  }
  
  @Override
  public void process(WatchedEvent event) {
    if (event.getType() == Event.EventType.NodeDataChanged) {
      try {
        Member node =  (Member) ZookeeperUtils.getNodeWritable(zk, path, memberClass, orbConf, this);
        leaderGroup.updateMembersData(memberPath, node);
      } catch (OrbZKFailure e) {
        e.printStackTrace();
        leaderGroup.fireEvent(new OrbExceptionEvent(e));
      }
    }
  }
  
}
