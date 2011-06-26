package org.goldenorb.zookeeper;

import org.goldenorb.conf.OrbConfigurable;

/**
 * The Barrier interface is to be implemented by another class for usage in GoldenOrb, i.e. OrbBarrier.
 * 
 */
public interface Barrier extends OrbConfigurable {
  
  public void enter() throws OrbZKFailure;
}
