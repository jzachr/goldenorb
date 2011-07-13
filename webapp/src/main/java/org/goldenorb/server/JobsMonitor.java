package org.goldenorb.server;

import java.util.List;
import java.util.Set;

import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * An implementation of Watcher to watch either the JobQueue node or JobsInProgress node and update an
 * OrbTrackerMemberDataServiceImpl servlet when the number of child nodes changes.
 */
public class JobsMonitor implements Watcher {
  
  private String jobsPath;
  private OrbTrackerMemberDataServiceImpl statusServer;
  private List<String> jobs;
  private ZooKeeper zk;
  private String nodeName;
  
  public JobsMonitor(String jobQueuePath, OrbTrackerMemberDataServiceImpl statusServer, ZooKeeper zk) throws OrbZKFailure {
    this.jobsPath = jobQueuePath;
    this.statusServer = statusServer;
    this.zk = zk;
    String splitPath[] = jobQueuePath.split("/");
    this.nodeName = splitPath[splitPath.length - 1];
    jobs = ZookeeperUtils.getChildren(zk, jobQueuePath, this);
    statusServer.updateJobs(jobs.toArray(new String[0]), nodeName);
  }
  
  @Override
  public void process(WatchedEvent event) {
    if (event.getType() != Event.EventType.NodeDeleted) {
      try {
        jobs = ZookeeperUtils.getChildren(zk, jobsPath, this);
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
          statusServer.updateJobs(jobs.toArray(new String[0]), nodeName);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else { // node was deleted return null
      String[] tmp = null;
      statusServer.updateJobs(tmp, nodeName);
      statusServer.watcherNodeDeleted(nodeName);
    }
  }
  
}
