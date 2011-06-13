package org.goldenorb.event;

import org.goldenorb.event.OrbEvent;

public class LeadershipChangeEvent extends OrbEvent {

  public LeadershipChangeEvent() {
    super(OrbEvent.LEADERSHIP_CHANGE);
  }
  
}
