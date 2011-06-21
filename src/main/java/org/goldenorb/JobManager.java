package org.goldenorb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.event.OrbCallback;
import org.goldenorb.event.OrbEvent;
import org.goldenorb.event.OrbExceptionEvent;
import org.goldenorb.event.job.JobDeathEvent;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;

public class JobManager implements OrbConfigurable {
  
  private String basePath;
  private String jobQueuePath;
  private String jobsInProgressPath;
  private OrbCallback orbCallback;
  private OrbConfiguration orbConf;
  private ZooKeeper zk;
  private JobsInQueueWatcher jobsInQueueWatcher;
  private SortedMap<String,OrbJob> jobs;
  private Set<String> activeJobs;
  private boolean activeManager = true;
  private String trackerID;
  public JobManager(OrbCallback orbCallback, OrbConfiguration orbConf, ZooKeeper zk) {
    activeJobs = new HashSet<String>();
    jobs = new TreeMap<String,OrbJob>();
    jobsInQueueWatcher = new JobsInQueueWatcher();
    this.orbConf = orbConf;
    this.orbCallback = orbCallback;
    basePath = OrbTracker.ZK_BASE_PATH + "/" + orbConf.getOrbClusterName();
    jobQueuePath = basePath + "/JobQueue";
    jobsInProgressPath = basePath + "/JobsInProgress";
    this.zk = zk;
    System.err.println("Initializing JobManager");
    buildJobManagerPaths();
    getJobsInQueue();
  }
  
  public int getJobTries(String jobNumber){
    synchronized(jobs){
      if(jobs.containsKey(jobNumber)){
        return jobs.get(jobNumber).getTries();
      } else {
        return -1;
      }
    }
  }
  
  public boolean isJobActive(String jobNumber){
    synchronized(activeJobs){
      return activeJobs.contains(jobNumber);
    }
  }
  
  private void getJobsInQueue() {
    System.out.println("getting jobs in queue.");
    synchronized (jobs) {
      List<String> jobQueueChildren = null;
      try {
        jobQueueChildren = zk.getChildren(jobQueuePath, jobsInQueueWatcher);
      } catch (KeeperException e) {
        fireEvent(new OrbExceptionEvent(e));
      } catch (InterruptedException e) {
        fireEvent(new OrbExceptionEvent(e));
      }
      List<String> jobsToRemove = new ArrayList<String>();
      for (String jobPath : jobs.keySet()) {
        if (!jobQueueChildren.contains(jobPath)) {
          jobsToRemove.add(jobPath);
          // Either a job has completed or been removed by someone else this should fire an event.
          // This should really not occur since it should only be removed by the JobManager itself.
          // In reality does an event really even need to be thrown?
        }
      }
      for(String job: jobsToRemove){
        System.err.println("Removing job: " + job);
        jobs.remove(job);
        activeJobs.remove(job);
      }
      for (String jobPath : jobQueueChildren) {
        OrbConfiguration jobConf;
        try {
          jobConf = (OrbConfiguration) ZookeeperUtils.getNodeWritable(zk, jobQueuePath + "/" + jobPath,
            OrbConfiguration.class, orbConf);
          if (jobConf != null) {
            if (!jobs.containsKey(jobPath)) {
              System.err.println("Adding job: " + jobPath);
              jobs.put(jobPath, new OrbJob(jobPath, jobConf));
              // Here we have a new job--once again an event should be fired.
              // Although I am not sure that an event really needs to be fired at this point. We will see.
            }
          } else {
            System.err.println("Job is not a valid job.");
          }
        } catch (OrbZKFailure e) {
          fireEvent(new OrbExceptionEvent(e));
        }
      }
    }
    tryToLaunchJob();
  }
  
  private void tryToLaunchJob() {
    synchronized (jobs) {
      if (!jobs.isEmpty()) {
        for (OrbJob job : jobs.values()) {
          System.err.println("Active Jobs: " + activeJobs);
          if (!activeJobs.contains(job.getJobNumber())) {
            if (resourcesAvailable(job)) {
              launchJob(job);
            }
          }
        }
      }
    }
  }
  
  private void launchJob(OrbJob job) {
    try {
      ZookeeperUtils.notExistCreateNode(zk, jobsInProgressPath + "/" + job.getJobNumber());
      ZookeeperUtils.notExistCreateNode(zk, jobsInProgressPath + "/" + job.getJobNumber()
                                            + "/OrbPartitionLeaderGroup");
      ZookeeperUtils.notExistCreateNode(zk, jobsInProgressPath + "/" + job.getJobNumber() + "/messages");
      ZookeeperUtils.tryToCreateNode(zk, jobsInProgressPath + "/" + job.getJobNumber()
                                         + "/messages/heartbeat", new LongWritable(0), CreateMode.PERSISTENT);
      JobStillActiveCheck jobStillActiveCheck = new JobStillActiveCheck(job);
      job.setJobStillActiveInterface(jobStillActiveCheck);
      new Thread(jobStillActiveCheck).start();
      activeJobs.add(job.getJobNumber());
      checkForDeathComplete(job);
      heartbeat(job);
      // TODO Allocate Resources to the job.
      System.err.println("Starting Job");
    } catch (OrbZKFailure e) {
      fireEvent(new OrbExceptionEvent(e));
    }
  }
  
  private void checkForDeathComplete(OrbJob job) throws OrbZKFailure {
    if (job.getDeathAndCompleteWatcher() == null) {
      job.setDeathAndCompleteWatcher(new DeathAndCompleteWatcher(job));
    }
    try {
      job.getDeathAndCompleteWatcher().restart();
      List<String> messages = zk.getChildren(jobsInProgressPath + "/" + job.getJobNumber() + "/messages",
        (Watcher) job.getDeathAndCompleteWatcher());
      if (messages.contains("death")) {
        jobDeath(job);
      }
      if (messages.contains("complete")) {
        jobComplete(job);
      }
    } catch (KeeperException e) {
      throw new OrbZKFailure(e);
    } catch (InterruptedException e) {
      throw new OrbZKFailure(e);
    }
    
  }
  
  public class DeathAndCompleteWatcher implements Killable, Watcher {
    
    private boolean active = true;
    private OrbJob job;
    
    public DeathAndCompleteWatcher(OrbJob job) {
      System.err.println("Creating DeathAndCompleteWatcher for: " + job.getJobNumber());
      this.job = job;
    }
    
    @Override
    public void process(WatchedEvent event) {
      if (active && activeManager) {
        try {
          System.err.println("DeathAndCompleteWatcher processing event for: " + job.getJobNumber());
          checkForDeathComplete(job);
        } catch (OrbZKFailure e) {
          fireEvent(new OrbExceptionEvent(e));
        }
      }
    }
    
    public void kill() {
      active = false;
    }
    
    public void restart() {
      active = true;
    }
  }
  
  public class HeartbeatWatcher implements Killable, Watcher {
    
    private OrbJob job;
    private boolean active = true;
    
    public HeartbeatWatcher(OrbJob job) {      
      System.err.println("Creating HeartbeatWatcher for: " + job.getJobNumber());
      this.job = job;
    }
    
    @Override
    public void process(WatchedEvent event) {
      if (active && activeManager) {
        try {
          System.err.println("HearbeatWatcher processing event for: " + job.getJobNumber());
          heartbeat(job);
        } catch (OrbZKFailure e) {
          fireEvent(new OrbExceptionEvent(e));
        }
      }
    }
    
    public void kill() {
      active = false;
    }
    
    public void restart(){
      active = true;
    }
  }
  
  private void heartbeat(OrbJob job) throws OrbZKFailure {
    if (job.getHeartbeatWatcher() == null) {
      job.setHeartbeatWatcher(new HeartbeatWatcher(job));
    }
    job.getHeartbeatWatcher().restart();
    Long newHeartbeat = ((LongWritable) ZookeeperUtils.getNodeWritable(zk,
      jobsInProgressPath + "/" + job.getJobNumber() + "/messages/heartbeat", LongWritable.class, orbConf,
      (Watcher) job.getHeartbeatWatcher())).get();
    System.out.println("Getting new heartbeat for: " + job.getJobNumber() + " has new heartbeat: " + newHeartbeat);
    job.setHeartbeat(newHeartbeat);
  }
  
  private boolean resourcesAvailable(OrbJob job) {
    return true;
  }
  
  private void removeJobFromQueue(OrbJob job) throws OrbZKFailure {
    ZookeeperUtils.deleteNodeIfEmpty(zk, jobQueuePath + "/" + job.getJobNumber());
  }
  
  private void jobDeath(OrbJob job) throws OrbZKFailure {
    synchronized(job){
      fireEvent(new JobDeathEvent(job.getJobNumber()));
      job.getJobStillActiveInterface().kill();
      job.getDeathAndCompleteWatcher().kill();
      job.getHeartbeatWatcher().kill();
      job.setJobStillActiveInterface(null);
    }
    
    // TODO tell other OrbTrackers to shut down their partition instances.
    System.err.println("Shutting down partition instances");
    System.err.println("Number of tries: " + job.getTries());
    if (job.getTries() > orbConf.getMaximumJobTries()) {
      ZookeeperUtils.recursiveDelete(zk, jobsInProgressPath + "/" + job.getJobNumber());
      ZookeeperUtils.deleteNodeIfEmpty(zk, jobsInProgressPath + "/" + job.getJobNumber());
      removeJobFromQueue(job);
    } else {
      ZookeeperUtils.recursiveDelete(zk, jobsInProgressPath + "/" + job.getJobNumber());
      ZookeeperUtils.deleteNodeIfEmpty(zk, jobsInProgressPath + "/" + job.getJobNumber());
      job.incrementTries();
      System.err.println("Incrementing tries for: " + job.getJobNumber());
    }
    synchronized(activeJobs){
      activeJobs.remove(job.getJobNumber());
      System.err.println("Removing job: " + job.getJobNumber() + " from activeJobs.");
    }
    tryToLaunchJob();
  }
  
  private void jobComplete(OrbJob job) throws OrbZKFailure {
    synchronized(job){
      job.getJobStillActiveInterface().kill();
      job.getDeathAndCompleteWatcher().kill();
      job.getHeartbeatWatcher().kill();
      job.setJobStillActiveInterface(null);
    }
    ZookeeperUtils.recursiveDelete(zk, jobsInProgressPath + "/" + job.getJobNumber());
    ZookeeperUtils.deleteNodeIfEmpty(zk, jobsInProgressPath + "/" + job.getJobNumber());
    removeJobFromQueue(job);
    tryToLaunchJob();
  }
  
  private class JobsInQueueWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      getJobsInQueue();
    }
  }
  
  private void fireEvent(OrbEvent orbEvent) {
    orbCallback.process(orbEvent);
  }
  
  private void buildJobManagerPaths() {
    try {
      ZookeeperUtils.notExistCreateNode(zk, jobQueuePath);
    } catch (OrbZKFailure e) {
      fireEvent(new OrbExceptionEvent(e));
    }
    try {
      ZookeeperUtils.notExistCreateNode(zk, jobsInProgressPath);
    } catch (OrbZKFailure e) {
      fireEvent(new OrbExceptionEvent(e));
    }
  }
  
  public class JobStillActiveCheck implements Killable, Runnable {
    
    private OrbJob job;
    private boolean active;
    private Long lastHeartbeat = -1L;
    
    public JobStillActiveCheck(OrbJob job) {
      System.err.println("Creating JobStillActiveChecker for: " + job.getJobNumber());
      this.job = job;
      active = true;
    }
    
    @Override
    public void run() {
      synchronized (this) {
        while (active && activeManager) {
          try {
            wait(orbConf.getJobHeartbeatTimeout());
          } catch (InterruptedException e) {
            fireEvent(new OrbExceptionEvent(e));
          }
          System.err.println("Checking heartbeat for: " + job.getJobNumber() + " Heartbeat is: " + job.getHeartbeat());
          if (job.getHeartbeat() <= lastHeartbeat) {
            try {
              jobDeath(job);
            } catch (OrbZKFailure e) {
              fireEvent(new OrbExceptionEvent(e));
            }
          }
          lastHeartbeat = job.getHeartbeat();
        }
      }
    }
    
    public boolean isActive() {
      return active;
    }
    
    public void setActive(boolean active) {
      this.active = active;
    }
    
    @Override
    public void kill() {
      active = false;
    }
    
    @Override
    public void restart() {
      active = true;
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
  
  public void shutdown() {
    activeManager = false;
  }
  
}
