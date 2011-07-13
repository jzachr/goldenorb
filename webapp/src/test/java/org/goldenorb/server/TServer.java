package org.goldenorb.server;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.goldenorb.client.OrbTrackerMemberData;

public class TServer extends OrbTrackerMemberDataServiceImpl {
  
  private static final long serialVersionUID = 2672656370931137360L;
  private String[] jobsInQueue;
  private String[] jobsInProgress;
  private Map<String,OrbTrackerMemberData> memberData = new HashMap<String,OrbTrackerMemberData>();
  private CountDownLatch latch;
  
  public TServer(CountDownLatch cdlatch) {
    super();
    this.latch = cdlatch;
  }
  
  @Override
  public synchronized void updateNodeData(OrbTrackerMemberData updatedNode) {
    memberData.put(updatedNode.getName(), updatedNode);
    try {
      latch.countDown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public synchronized void removeNodeData(String nodeName) {
    memberData.remove(nodeName);
    try {
      latch.countDown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public Map<String,OrbTrackerMemberData> getMemberData() {
    return memberData;
  }
  
  public void setLatcher(CountDownLatch latch) {
    this.latch = latch;
  }
  
  @Override
  public void updateJobs(String[] jobs, String nodeName) {
    if (nodeName.equalsIgnoreCase("JobQueue")) {
      jobsInQueue = jobs;
    } else {
      jobsInProgress = jobs;
    }
    latch.countDown();
  }
  
  public String[] getJobsInProgress() {
    return jobsInProgress;
  }
  
  public String[] getJobsInQueue() {
    return jobsInQueue;
  }
  
}
