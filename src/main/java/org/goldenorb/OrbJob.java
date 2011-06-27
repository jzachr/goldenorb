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
 * 
 */
package org.goldenorb;

import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;

/**
 * OrbJob encompasses the pieces necessary for JobManager to start/watch/complete/kill jobs
 */

public class OrbJob implements OrbConfigurable, Comparable<OrbJob> {
  
  private String jobNumber;
  private JobStatus status;
  private OrbConfiguration orbConf;
  private Killable deathAndCompleteWatcher;
  private Killable heartbeatWatcher;
  private Long heartbeat;
  private Killable jobStillActiveInterface;
  private int tries;
  
/**
 * Constructor
 *
 * @param  String jobPath
 * @param  OrbConfiguration jobConf
 */
  public OrbJob(String jobPath, OrbConfiguration jobConf) {
    this.jobNumber = jobPath;
    jobConf.setJobNumber(jobPath);
    this.orbConf = jobConf;
    heartbeat = 0L;
    tries = 1;
  }
  
/**
 * 
 * @param  OrbJob rhs
 * @returns int
 */
  @Override
  public int compareTo(OrbJob rhs) {
    return jobNumber.compareTo(rhs.getJobNumber());
  }
  
/**
 * Set the jobNumber
 * @param  String jobNumber
 */
  public void setJobNumber(String jobNumber) {
    this.jobNumber = jobNumber;
  }
  
/**
 * Return the jobNumber
 */
  public String getJobNumber() {
    return jobNumber;
  }
  
/**
 * Set the status
 * @param  JobStatus status
 */
  public void setStatus(JobStatus status) {
    this.status = status;
  }
  
/**
 * Return the status
 */
  public JobStatus getStatus() {
    return status;
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
 * Set the deathAndCompleteWatcher
 * @param  Killable deathAndCompleteWatcher
 */
  public void setDeathAndCompleteWatcher(Killable deathAndCompleteWatcher) {
    this.deathAndCompleteWatcher = deathAndCompleteWatcher;
  }
  
/**
 * Return the deathAndCompleteWatcher
 */
  public Killable getDeathAndCompleteWatcher() {
    return deathAndCompleteWatcher;
  }
  
/**
 * Set the heartbeatWatcher
 * @param  Killable heartbeatWatcher
 */
  public void setHeartbeatWatcher(Killable heartbeatWatcher) {
    this.heartbeatWatcher = heartbeatWatcher;
  }
  
/**
 * Return the heartbeatWatcher
 */
  public Killable getHeartbeatWatcher() {
    return heartbeatWatcher;
  }
  
/**
 * Set the heartbeat
 * @param  Long heartbeat
 */
  public void setHeartbeat(Long heartbeat) {
    this.heartbeat = heartbeat;
  }
  
/**
 * Return the heartbeat
 */
  public Long getHeartbeat() {
    return heartbeat;
  }
  
/**
 * Set the jobStillActiveInterface
 * @param  Killable jobStillActiveInterface
 */
  public void setJobStillActiveInterface(Killable jobStillActiveInterface) {
    this.jobStillActiveInterface = jobStillActiveInterface;
  }
  
/**
 * Return the jobStillActiveInterface
 */
  public Killable getJobStillActiveInterface() {
    return jobStillActiveInterface;
  }
  
/**
 * Set the tries
 * @param  int tries
 */
  public void setTries(int tries) {
    this.tries = tries;
  }
  
/**
 * Return the tries
 */
  public int getTries() {
    return tries;
  }
  
/**
 * 
 */
  public void incrementTries() {
    tries++;
  }
  
}
