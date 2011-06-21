package org.goldenorb.zookeeper;

import org.goldenorb.conf.OrbConfigurable;

public interface Barrier extends OrbConfigurable {
  
  public void enter() throws OrbZKFailure;
}
