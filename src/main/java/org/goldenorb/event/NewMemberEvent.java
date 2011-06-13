package org.goldenorb.event;

import org.goldenorb.event.OrbEvent;

public class NewMemberEvent extends OrbEvent {

  public NewMemberEvent() {
    super(OrbEvent.NEW_MEMBER);
  }

}
