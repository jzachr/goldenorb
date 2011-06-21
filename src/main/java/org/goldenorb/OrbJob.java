package org.goldenorb;

import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;

public class OrbJob implements OrbConfigurable, Comparable<OrbJob> {
  
  private String jobNumber;
  private JobStatus status;
  private OrbConfiguration orbConf;
  private Killable deathAndCompleteWatcher;
  private Killable heartbeatWatcher;
  private Long heartbeat;
  private Killable jobStillActiveInterface;
  private int tries;
  
  public OrbJob(String jobPath, OrbConfiguration jobConf) {
    this.jobNumber = jobPath;
    jobConf.setJobNumber(jobPath);
    this.orbConf = jobConf;
    heartbeat = 0L;
    tries = 1;
  }
  
  @Override
  public int compareTo(OrbJob rhs) {
    return jobNumber.compareTo(rhs.getJobNumber());
  }
  
  public void setJobNumber(String jobNumber) {
    this.jobNumber = jobNumber;
  }
  
  public String getJobNumber() {
    return jobNumber;
  }
  
  public void setStatus(JobStatus status) {
    this.status = status;
  }
  
  public JobStatus getStatus() {
    return status;
  }
  
  @Override
  public void setOrbConf(OrbConfiguration orbConf) {
    this.orbConf = orbConf;
  }
  
  @Override
  public OrbConfiguration getOrbConf() {
    return orbConf;
  }
  
  public void setDeathAndCompleteWatcher(Killable deathAndCompleteWatcher) {
    this.deathAndCompleteWatcher = deathAndCompleteWatcher;
  }
  
  public Killable getDeathAndCompleteWatcher() {
    return deathAndCompleteWatcher;
  }
  
  public void setHeartbeatWatcher(Killable heartbeatWatcher) {
    this.heartbeatWatcher = heartbeatWatcher;
  }
  
  public Killable getHeartbeatWatcher() {
    return heartbeatWatcher;
  }
  
  public void setHeartbeat(Long heartbeat) {
    this.heartbeat = heartbeat;
  }
  
  public Long getHeartbeat() {
    return heartbeat;
  }
  
  public void setJobStillActiveInterface(Killable jobStillActiveInterface) {
    this.jobStillActiveInterface = jobStillActiveInterface;
  }
  
  public Killable getJobStillActiveInterface() {
    return jobStillActiveInterface;
  }
  
  public void setTries(int tries) {
    this.tries = tries;
  }
  
  public int getTries() {
    return tries;
  }
  
  public void incrementTries() {
    tries++;
  }
  
}
