package org.goldenorb.server;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.goldenorb.client.OrbTrackerMemberData;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.jet.OrbTrackerMember;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;

/**
 * An implementation of Watcher that watches the child nodes of the LeaderGroup node in ZooKeeper and notifies
 * an OrbTrackermemberDataServiceImpl servlet when ever the data of those nodes change.
 */
public class LeaderGroupMemberMonitor implements Watcher {
  
  private OrbTrackerMember orbTrackerMember;
  private ZooKeeper zk;
  private String path;
  private String name;
  private OrbTrackerMemberDataServiceImpl statusServer;
  
  public LeaderGroupMemberMonitor(String path,
                                  ZooKeeper zk,
                                  String name,
                                  OrbTrackerMemberDataServiceImpl statusServer) throws OrbZKFailure {
    this.path = path;
    this.name = name;
    this.zk = zk;
    this.statusServer = statusServer;
    orbTrackerMember = (OrbTrackerMember) ZookeeperUtils.getNodeWritable(zk, path, new OrbTrackerMember(),
      this);
    if (orbTrackerMember != null) {
      upDateServer();
    }
  }
  
  @Override
  public void process(WatchedEvent event) {
    if (event.getType() != Event.EventType.NodeDeleted) {
      try {
        orbTrackerMember = (OrbTrackerMember) ZookeeperUtils.getNodeWritable(zk, path,
          new OrbTrackerMember(), this);
      } catch (OrbZKFailure e) {
        System.err.println("ERROR RETRIEVING MEMBER NODE DATA FROM ZOOKER : " + path);
        e.printStackTrace();
      }
      if (event.getType() == Event.EventType.NodeDataChanged && orbTrackerMember != null) {
        upDateServer();
      }
    } else { // node was deleted
      statusServer.removeNodeData(name);
    }
  }
  
  private void upDateServer() {
    statusServer.updateNodeData(new OrbTrackerMemberData(name, orbTrackerMember.getPartitionCapacity(),
        orbTrackerMember.getAvailablePartitions(), orbTrackerMember.getReservedPartitions(), orbTrackerMember
            .getInUsePartitions(), orbTrackerMember.getHostname(), orbTrackerMember.isLeader(),
        orbTrackerMember.getPort()));
  }
  
}
