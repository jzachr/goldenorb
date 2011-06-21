package org.goldenorb.event.job;

import org.goldenorb.event.OrbEvent;

public class JobStartEvent extends OrbEvent {

  public JobStartEvent() {
    super(OrbEvent.JOB_START);
  }
}
