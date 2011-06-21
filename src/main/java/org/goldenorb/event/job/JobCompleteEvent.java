package org.goldenorb.event.job;

import org.goldenorb.event.OrbEvent;

public class JobCompleteEvent extends OrbEvent {

  private String jobNumber;
  
  public JobCompleteEvent(String jobNumber) {
    super(OrbEvent.JOB_COMPLETE);
  }
}
