package org.goldenorb.server;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.client.NodeDoesNotExistException;
import org.goldenorb.client.OrbTrackerMemberData;
import org.goldenorb.client.OrbTrackerMemberDataService;
import org.goldenorb.client.WatcherException;
import org.goldenorb.client.ZooKeeperConnectionException;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;

/**
 * GWT servlet that watches for status update through ZooKeeper.
 */
public class OrbTrackerMemberDataServiceImpl extends RemoteServiceServlet implements
    OrbTrackerMemberDataService, Watcher {
  
  /**
   * 
   */
  private static final long serialVersionUID = 7790444401752275415L;
  private Map<String,OrbTrackerMemberData> memberDataContainer = new HashMap<String,OrbTrackerMemberData>();
  private String[] jobsInQueue;
  private String[] jobsInProgress;
  private ZooKeeper zk = null;
  // private String cluster;
  private Watcher leaderGroupWatcher;
  private Watcher jobQueueWatcher;
  private Watcher jobsInProgressWatcher;
  private OrbConfiguration orbConf = new OrbConfiguration(true);
  private boolean testingMode = false;
  private CountDownLatch jobsLatch;
  private CountDownLatch removeLatch;
  private CountDownLatch updateLatch;
  
  /**
   * Returns the data of the OrbTrackerMember nodes under the LeaderGroup node in ZooKeeper.
   */
  @Override
  public OrbTrackerMemberData[] getOrbTrackerMemberData() throws ZooKeeperConnectionException,
                                                         WatcherException,
                                                         NodeDoesNotExistException {
    if (zk == null) {
      try {
        initializeZooKeeper();
        memberDataContainer = new HashMap<String,OrbTrackerMemberData>();
      } catch (Exception e) {
        e.printStackTrace();
        throw new ZooKeeperConnectionException(e);
      }
    }
    // Initialize leader group watcher
    if (leaderGroupWatcher == null) {
      try {
        initializeLeaderGroupMonitor();
      } catch (OrbZKFailure e) {
        throw new WatcherException(e);
      }
    }
    return memberDataContainer.values().toArray(new OrbTrackerMemberData[0]);
  }
  
  private void initializeLeaderGroupMonitor() throws OrbZKFailure, NodeDoesNotExistException {
    String leaderGroupPath = "/GoldenOrb/" + orbConf.getOrbClusterName() + "/OrbTrackerLeaderGroup";
    if (ZookeeperUtils.nodeExists(zk, leaderGroupPath)) {
      leaderGroupWatcher = new LeaderGroupMonitor(leaderGroupPath, this, zk);
    } else {
      throw new NodeDoesNotExistException(leaderGroupPath);
    }
  }
  
  private void initializeJobMonitor(String nodeName) throws NodeDoesNotExistException, OrbZKFailure {
    String jobPath = "/GoldenOrb/" + orbConf.getOrbClusterName() + "/" + nodeName;
    if (ZookeeperUtils.nodeExists(zk, jobPath)) {
      if (nodeName.equalsIgnoreCase("JobQueue")) jobQueueWatcher = new JobsMonitor(jobPath, this, zk);
      else jobsInProgressWatcher = new JobsMonitor(jobPath, this, zk);
    } else {
      throw new NodeDoesNotExistException(jobPath);
    }
  }
  
  /**
   * Initializes a ZooKeeper instance if it has not already been intialized.
   * 
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  private void initializeZooKeeper() throws IOException, InterruptedException {
    // String connectString = getServletConfig().getInitParameter("connectString");
	if(testingMode) {
		orbConf.setOrbZooKeeperQuorum("localhost:21810");
	}
    zk = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
  }
  
  /**
   * Update the data of the OrbTrackerMember nodes being monitored.
   * 
   * @param updatedNode
   *          - OrbTrackermemberData object
   */
  public synchronized void updateNodeData(OrbTrackerMemberData updatedNode) {
    memberDataContainer.put(updatedNode.getName(), updatedNode);
    if (testingMode) {
      updateLatch.countDown();
    }
  }
  
  /**
   * Remove a node from the data container that is sent to client side application.
   * 
   * @param nodeName
   */
  public synchronized void removeNodeData(String nodeName) {
    memberDataContainer.remove(nodeName);
    if (testingMode) {
      removeLatch.countDown();
    }
  }
  
  @Override
  public void process(WatchedEvent event) {
    // Don't care about what event get passed back from connecting to zookeeper.
    // It will either connect or throw an exception.
  }
  
  /**
   * Returns an array of the job names that are in queue.
   */
  @Override
  public String[] getJobsInQueue() throws NodeDoesNotExistException,
                                  ZooKeeperConnectionException,
                                  WatcherException {
    if (zk == null) {
      try {
        initializeZooKeeper();
      } catch (Exception e) {
        throw new ZooKeeperConnectionException(e);
      }
    }
    if (jobQueueWatcher == null) {
      try {
        initializeJobMonitor("JobQueue");
      } catch (OrbZKFailure e) {
        throw new WatcherException(e);
      }
    }
    return jobsInQueue;
  }
  
  /**
   * Returns an array of job names that are in progress.
   */
  @Override
  public String[] getJobsInProgress() throws NodeDoesNotExistException,
                                     ZooKeeperConnectionException,
                                     WatcherException {
    if (zk == null) {
      try {
        initializeZooKeeper();
      } catch (Exception e) {
        throw new ZooKeeperConnectionException(e);
      }
    }
    if (jobsInProgressWatcher == null) {
      try {
        initializeJobMonitor("JobsInProgress");
      } catch (OrbZKFailure e) {
        throw new WatcherException(e);
      }
    }
    return jobsInProgress;
  }
  
  /**
   * Sets the jobs that are in queue or in progress.
   * 
   * @param jobs
   * @param nodeName
   */
  public void updateJobs(String[] jobs, String nodeName) {
    if (nodeName.equalsIgnoreCase("JobQueue")) {
      jobsInQueue = jobs;
    } else {
      jobsInProgress = jobs;
    }
    if (testingMode) {
      jobsLatch.countDown();
    }
  }
  
  /**
   * Also the use of countdownlatches to monitor when the servlet is receiving updates for testing purposes.
   * 
   * @param jobsLatch
   * @param removeLatch
   * @param updateLatch
   */
  public void enterTestingMode(CountDownLatch jobsLatch,
                               CountDownLatch removeLatch,
                               CountDownLatch updateLatch) {
    this.jobsLatch = jobsLatch;
    this.removeLatch = removeLatch;
    this.updateLatch = updateLatch;
    this.testingMode = true;
  }
  
  /**
   * Used for testing purposes only
   */
  public void exitTestingMode() {
    this.testingMode = false;
  }
  
  /**
   * Used for testing purposes only
   */
  public void setUpdateLatch(CountDownLatch updateLatch) {
    this.updateLatch = updateLatch;
  }
  
  /**
   * Used for testing purposes only
   */
  public void setRemoveLatch(CountDownLatch removeLatch) {
    this.removeLatch = removeLatch;
  }
  
  /**
   * Used for testing purposes only
   */
  public void setJobsLatch(CountDownLatch jobsLatch) {
    this.jobsLatch = jobsLatch;
  }
  
  /**
   * Used for testing purposes only
   */
  public void setLatches(CountDownLatch jobsLatch, CountDownLatch removeLatch, CountDownLatch updateLatch) {
    this.jobsLatch = jobsLatch;
    this.removeLatch = removeLatch;
    this.updateLatch = updateLatch;
  }
  
  /**
   * Notify the servlet that node that should be watched has been deleted, so it will notice the creation of a
   * new node of the same type.
   */
  public void watcherNodeDeleted(String node) {
    if (node.equalsIgnoreCase("OrbTrackerLeaderGroup")) {
      leaderGroupWatcher = null;
      memberDataContainer.clear();
    } else if (node.equalsIgnoreCase("JobQueue")) {
      jobQueueWatcher = null;
      jobsInQueue = new String[0];
    } else if (node.equalsIgnoreCase("JobsInProgress")) {
      jobsInProgressWatcher = null;
      jobsInProgress = new String[0];
    }
  }
  
}
