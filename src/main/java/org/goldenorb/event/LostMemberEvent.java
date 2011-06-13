package org.goldenorb.event;

import org.goldenorb.event.OrbEvent;

public class LostMemberEvent extends OrbEvent {

  public LostMemberEvent() {
    super(OrbEvent.LOST_MEMBER);
  }
}
