package org.goldenorb.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;

/**
 * An implementation of Watcher that watches the LeaderGroup node in ZooKeeper and notifies an
 * OrbTrackermemberDataServiceImpl servlet when ever the number of child nodes change.
 */
public class LeaderGroupMonitor implements Watcher {
  
  private ZooKeeper zk;
  private List<String> members;
  private String path;
  private OrbTrackerMemberDataServiceImpl statusServer;
  public boolean dead;
  private Map<String,LeaderGroupMemberMonitor> memberWatchers;
  
  public LeaderGroupMonitor(String path, OrbTrackerMemberDataServiceImpl statusServer, ZooKeeper zk) throws OrbZKFailure {
    this.zk = zk;
    this.path = path;
    this.statusServer = statusServer;
    memberWatchers = new HashMap<String,LeaderGroupMemberMonitor>();
    members = ZookeeperUtils.getChildren(zk, path, this);
    for (String node : members) {
      memberWatchers.put(node, new LeaderGroupMemberMonitor(path + "/" + node, zk, node, statusServer));
    }
  }
  
  @Override
  public void process(WatchedEvent event) {
    if (event.getType() != Event.EventType.NodeDeleted) {
      try {
        members = ZookeeperUtils.getChildren(zk, path, this);
      } catch (OrbZKFailure e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      if (event.getType() == Event.EventType.NodeChildrenChanged) {
        for (String node : members) {
          if (!memberWatchers.containsKey(node)) {
            try {
              memberWatchers.put(node,
                new LeaderGroupMemberMonitor(path + "/" + node, zk, node, statusServer));
            } catch (OrbZKFailure e) {
              System.err.println("ERROR CREATING WATCHER FOR : " + path + "/" + node);
              e.printStackTrace();
            }
          }
        }
        Set<String> watcherSet = memberWatchers.keySet();
        List<String> toRemove = new ArrayList<String>();
        for (String watcher : watcherSet) {
          if (!members.contains(watcher)) {
            toRemove.add(watcher);
          }
        }
        for (String nodeToRemove : toRemove) {
          memberWatchers.remove(nodeToRemove);
        }
      }
    } else {
      statusServer.watcherNodeDeleted("OrbTrackerLeaderGroup");
    }
  }
  
}
