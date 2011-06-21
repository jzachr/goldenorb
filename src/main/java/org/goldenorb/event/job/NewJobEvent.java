package org.goldenorb.event.job;

import org.goldenorb.event.OrbEvent;

public class NewJobEvent extends OrbEvent {

  private String jobNumber;
  
  public NewJobEvent(String jobNumber) {
    super(OrbEvent.NEW_JOB);
    this.jobNumber = jobNumber;
  }

  public void setJobNumber(String jobNumber) {
    this.jobNumber = jobNumber;
  }

  public String getJobNumber() {
    return jobNumber;
  }

}
