/**
 * Licensed to Ravel, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Ravel, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.goldenorb;

import java.io.IOException;
import java.util.ArrayList;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InvalidJobConfException;
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
import org.goldenorb.jet.OrbTrackerMember;
import org.goldenorb.jet.PartitionRequest;
import org.goldenorb.util.ResourceAllocator;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobManager is created by the lead OrbTracker to aid in putting jobs-in-the-queue and pulling
 * them into jobs-in-progress. It tells the OrbTrackerMembers in the LeaderGroup to launch their
 * partitions. Then it watches the jobs (via heart beat field in ZooKeeper) to make sure they're 
 * still running,   
 *
 * @param <M> The class of type that JobManager will actually manage
 */
public class JobManager<M extends OrbTrackerMember> implements OrbConfigurable {
  
  private final Logger logger = LoggerFactory.getLogger(JobManager.class);
  
  private String basePath;
  private String jobQueuePath;
  private String jobsInProgressPath;
  
  private OrbCallback orbCallback;
  private OrbConfiguration orbConf;
  private ZooKeeper zk;
  private ResourceAllocator<M> resourceAllocator;
  private Collection<M> orbTrackerMembers;
  
  private JobsInQueueWatcher jobsInQueueWatcher = new JobsInQueueWatcher();
  private SortedMap<String,OrbJob> jobs = new TreeMap<String,OrbJob>();
  private Set<String> activeJobs = new HashSet<String>();
  
  private boolean activeManager = true;
  
/**
 * Constructor
 *
 * @param  OrbCallback orbCallback
 * @param  OrbConfiguration orbConf
 * @param  ZooKeeper zk
 * @param  ResourceAllocator<M> resourceAllocator
 * @param  Collection<M> orbTrackers
 */
  public JobManager(OrbCallback orbCallback,
                    OrbConfiguration orbConf,
                    ZooKeeper zk,
                    ResourceAllocator<M> resourceAllocator,
                    Collection<M> orbTrackers) {
    logger.info("Initializing JobManager");
    
    this.orbConf = orbConf;
    this.orbCallback = orbCallback;
    this.zk = zk;
    this.resourceAllocator = resourceAllocator;
    this.orbTrackerMembers = orbTrackers;
    
    basePath = OrbTracker.ZK_BASE_PATH + "/" + orbConf.getOrbClusterName();
    jobQueuePath = basePath + "/JobQueue";
    jobsInProgressPath = basePath + "/JobsInProgress";
    
    buildJobManagerPaths();
    getJobsInQueue();
  }
  
/**
 * Return the jobTries
 */
  public int getJobTries(String jobNumber) {
    synchronized (jobs) {
      if (jobs.containsKey(jobNumber)) {
        return jobs.get(jobNumber).getTries();
      } else {
        return -1;
      }
    }
  }
  
/**
 * Return the obActive
 */
  public boolean isJobActive(String jobNumber) {
    synchronized (activeJobs) {
      return activeJobs.contains(jobNumber);
    }
  }
  
/**
 * Return the jobsInQueue
 */
  private void getJobsInQueue() {
    logger.info("getting jobs in queue.");
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
      for (String job : jobsToRemove) {
        logger.debug("Removing job: " + job);
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
              logger.debug("Adding job: " + jobPath);
              jobs.put(jobPath, new OrbJob(jobPath, jobConf));
              // Here we have a new job--once again an event should be fired.
              // Although I am not sure that an event really needs to be fired at this point. We will see.
            }
          } else {
            logger.debug("Job is not a valid job.");
          }
        } catch (OrbZKFailure e) {
          fireEvent(new OrbExceptionEvent(e));
        }
      }
    }
    tryToLaunchJob();
  }
  
/**
 * 
 */
  private void tryToLaunchJob() {
    synchronized (jobs) {
      if (!jobs.isEmpty()) {
        for (OrbJob job : jobs.values()) {
          logger.debug("Active Jobs: " + activeJobs);
          if (!activeJobs.contains(job.getJobNumber())) {
            if (resourcesAvailable(job)) {
              launchJob(job);
            }
          }
        }
      }
    }
  }
  
/**
 * 
 * @param  OrbJob job
 */
  private void launchJob(OrbJob job) {
    try {
      ZookeeperUtils.notExistCreateNode(zk, jobsInProgressPath + "/" + job.getJobNumber());
      ZookeeperUtils.notExistCreateNode(zk, jobsInProgressPath + "/" + job.getJobNumber()
                                            + "/OrbPartitionLeaderGroup");
      ZookeeperUtils.notExistCreateNode(zk, jobsInProgressPath + "/" + job.getJobNumber() + "/messages");
      ZookeeperUtils.tryToCreateNode(zk, jobsInProgressPath + "/" + job.getJobNumber()
                                         + "/messages/heartbeat", new LongWritable(0), CreateMode.PERSISTENT);
      
      
      
      // allocate resources and if enough, start the job
      logger.info("checking for available OrbTracker resources");
      Map<M,Integer[]> assignments = null;
      try {
        assignments = resourceAllocator.assignResources(job.getOrbConf());
      } catch (InvalidJobConfException e) {
        logger.error(e.getMessage());
      }
      logger.info("Starting Job");
      if (assignments != null) {
        logger.info("Allocating partitions");
        
        int basePartitionID = 0;
        for (M tracker : orbTrackerMembers) {
          logger.debug("OrbTracker - " + tracker.getHostname() + ":" + tracker.getPort());
          Integer[] assignment = assignments.get(tracker);
          
          PartitionRequest request = new PartitionRequest();
          request.setActivePartitions(assignment[ResourceAllocator.TRACKER_AVAILABLE]);
          request.setReservedPartitions(assignment[ResourceAllocator.TRACKER_RESERVED]);
          request.setJobID(job.getJobNumber());
          request.setBasePartitionID(basePartitionID);
          basePartitionID += assignment[ResourceAllocator.TRACKER_AVAILABLE];
          
          logger.debug("requesting partitions");
          tracker.initProxy(getOrbConf());
          tracker.requestPartitions(request);
          logger.info(request.toString());
          
          JobStillActiveCheck jobStillActiveCheck = new JobStillActiveCheck(job);
          job.setJobStillActiveInterface(jobStillActiveCheck);
          new Thread(jobStillActiveCheck).start();
          
          activeJobs.add(job.getJobNumber());
          checkForDeathComplete(job);
          heartbeat(job);
        }
      } else {
        logger.error("not enough capacity for this job");
        jobComplete(job);
      }
    } catch (OrbZKFailure e) {
      e.printStackTrace();
      logger.error(e.getMessage());
      fireEvent(new OrbExceptionEvent(e));
    } //catch (IOException e) {
//      e.printStackTrace();
//      logger.error(e.getMessage());
//    }
  }
  
/**
 * 
 * @param  OrbJob job
 */
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
    
/**
 * Constructor
 *
 * @param  OrbJob job
 */
    public DeathAndCompleteWatcher(OrbJob job) {
      logger.info("Creating DeathAndCompleteWatcher for: " + job.getJobNumber());
      this.job = job;
    }
    
/**
 * 
 * @param  WatchedEvent event
 */
    @Override
    public void process(WatchedEvent event) {
      if (active && activeManager) {
        try {
          logger.debug("DeathAndCompleteWatcher processing event for: " + job.getJobNumber());
          checkForDeathComplete(job);
        } catch (OrbZKFailure e) {
          fireEvent(new OrbExceptionEvent(e));
        }
      }
    }
    
/**
 * 
 */
    public void kill() {
      active = false;
    }
    
/**
 * 
 */
    public void restart() {
      active = true;
    }
  }
  
  public class HeartbeatWatcher implements Killable, Watcher {
    
    private OrbJob job;
    private boolean active = true;
    
/**
 * Constructor
 *
 * @param  OrbJob job
 */
    public HeartbeatWatcher(OrbJob job) {
      logger.debug("Creating HeartbeatWatcher for: " + job.getJobNumber());
      this.job = job;
    }
    
/**
 * 
 * @param  WatchedEvent event
 */
    @Override
    public void process(WatchedEvent event) {
      if (active && activeManager) {
        try {
          logger.debug("HearbeatWatcher processing event for: " + job.getJobNumber());
          heartbeat(job);
        } catch (OrbZKFailure e) {
          fireEvent(new OrbExceptionEvent(e));
        }
      }
    }
    
/**
 * 
 */
    public void kill() {
      active = false;
    }
    
/**
 * 
 */
    public void restart() {
      active = true;
    }
  }
  
/**
 * 
 * @param  OrbJob job
 */
  private void heartbeat(OrbJob job) throws OrbZKFailure {
    if (job.getHeartbeatWatcher() == null) {
      job.setHeartbeatWatcher(new HeartbeatWatcher(job));
    }
    job.getHeartbeatWatcher().restart();
    Long newHeartbeat = ((LongWritable) ZookeeperUtils.getNodeWritable(zk,
      jobsInProgressPath + "/" + job.getJobNumber() + "/messages/heartbeat", LongWritable.class, orbConf,
      (Watcher) job.getHeartbeatWatcher())).get();
    logger.debug("Getting new heartbeat for: " + job.getJobNumber() + " has new heartbeat: " + newHeartbeat);
    job.setHeartbeat(newHeartbeat);
  }
  
/**
 * 
 * @param  OrbJob job
 * @returns boolean
 */
  private boolean resourcesAvailable(OrbJob job) {
    // TODO what do we need to examine in order to actually check whether
    // resources are available?
    return true;
  }
  
/**
 * 
 * @param  OrbJob job
 */
  private void removeJobFromQueue(OrbJob job) throws OrbZKFailure {
    ZookeeperUtils.deleteNodeIfEmpty(zk, jobQueuePath + "/" + job.getJobNumber());
  }
  
/**
 * 
 * @param  OrbJob job
 */
  private void jobDeath(OrbJob job) throws OrbZKFailure {
    logger.info("jobDeath " + job.getJobNumber());
    synchronized (job) {
      fireEvent(new JobDeathEvent(job.getJobNumber()));
      job.getJobStillActiveInterface().kill();
      job.getDeathAndCompleteWatcher().kill();
      job.getHeartbeatWatcher().kill();
    }
    
    for(OrbTrackerMember orbTrackerMember: orbTrackerMembers){
      orbTrackerMember.killJob(job.getJobNumber());
    }
    logger.info("Shutting down partition instances");
    logger.info("Number of tries: " + job.getTries());
    if (job.getTries() > orbConf.getMaximumJobTries()) {
      ZookeeperUtils.recursiveDelete(zk, jobsInProgressPath + "/" + job.getJobNumber());
      ZookeeperUtils.deleteNodeIfEmpty(zk, jobsInProgressPath + "/" + job.getJobNumber());
      removeJobFromQueue(job);
    } else {
      ZookeeperUtils.recursiveDelete(zk, jobsInProgressPath + "/" + job.getJobNumber());
      ZookeeperUtils.deleteNodeIfEmpty(zk, jobsInProgressPath + "/" + job.getJobNumber());
      job.incrementTries();
      logger.info("Incrementing tries for: " + job.getJobNumber());
    }
    synchronized (activeJobs) {
      activeJobs.remove(job.getJobNumber());
      logger.info("Removing job: " + job.getJobNumber() + " from activeJobs.");
      // TODO tell the other OrbTrackers to kill the partitions associated with the job.
    }
    tryToLaunchJob();
  }
  
/**
 * 
 * @param  OrbJob job
 */
  private void jobComplete(OrbJob job) throws OrbZKFailure {
    synchronized (job) {
      job.getJobStillActiveInterface().kill();
      job.getDeathAndCompleteWatcher().kill();
      job.getHeartbeatWatcher().kill();
    }
    
    for(OrbTrackerMember orbTrackerMember: orbTrackerMembers){
      orbTrackerMember.killJob(job.getJobNumber());
    }

    ZookeeperUtils.recursiveDelete(zk, jobsInProgressPath + "/" + job.getJobNumber());
    ZookeeperUtils.deleteNodeIfEmpty(zk, jobsInProgressPath + "/" + job.getJobNumber());
    removeJobFromQueue(job);
    tryToLaunchJob();
  }
  
  private class JobsInQueueWatcher implements Watcher {
/**
 * 
 * @param  WatchedEvent event
 */
    @Override
    public void process(WatchedEvent event) {
      getJobsInQueue();
    }
  }
  
/**
 * 
 * @param  OrbEvent orbEvent
 */
  private void fireEvent(OrbEvent orbEvent) {
    orbCallback.process(orbEvent);
  }
  
/**
 * 
 */
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
    
/**
 * Constructor
 *
 * @param  OrbJob job
 */
    public JobStillActiveCheck(OrbJob job) {
      logger.info("Creating JobStillActiveChecker for: " + job.getJobNumber());
      this.job = job;
      active = true;
    }
    
/**
 * 
 */
    @Override
    public void run() {
      synchronized (this) {
        while (active && activeManager) {
          try {
            wait(orbConf.getJobHeartbeatTimeout());
          } catch (InterruptedException e) {
            fireEvent(new OrbExceptionEvent(e));
          }
          logger.debug("Checking heartbeat for: " + job.getJobNumber() + " Heartbeat is: "
                       + job.getHeartbeat());
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
    
/**
 * Return the ctive
 */
    public boolean isActive() {
      return active;
    }
    
/**
 * Set the active
 * @param  boolean active
 */
    public void setActive(boolean active) {
      this.active = active;
    }
    
/**
 * 
 */
    @Override
    public void kill() {
      active = false;
    }
    
/**
 * 
 */
    @Override
    public void restart() {
      active = true;
    }
  }
  
/**
 * Set the orbConf
 * @param  OrbConfiguration orbConf
 */
  @Override
  public void setOrbConf(OrbConfiguration orbConf) {
    this.orbConf = orbConf;
  }
  
/**
 * Return the orbConf
 */
  @Override
  public OrbConfiguration getOrbConf() {
    return orbConf;
  }
  
/**
 * 
 */
  public void shutdown() {
    activeManager = false;
  }
  
}
