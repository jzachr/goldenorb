package org.goldenorb.event.job;

import org.goldenorb.event.OrbEvent;

public class JobDeathEvent extends OrbEvent{
  private String jobNumber;
  public JobDeathEvent(String jobNumber) {
    super(OrbEvent.JOB_DEATH);
    this.jobNumber = jobNumber;
  }
  public void setJobNumber(String jobNumber) {
    this.jobNumber = jobNumber;
  }
  public String getJobNumber() {
    return jobNumber;
  }
}
