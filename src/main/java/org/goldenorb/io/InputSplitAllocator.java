package org.goldenorb.io;

import java.util.List;
import java.util.Map;

import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.io.input.RawSplit;
import org.goldenorb.jet.OrbTrackerMember;

public class InputSplitAllocator implements OrbConfigurable {
  
  private OrbConfiguration orbConf;
  private List<OrbTrackerMember> orbTrackerMembers;
  public InputSplitAllocator(OrbConfiguration orbConf, List<OrbTrackerMember> orbTrackerMembers ){
    this.orbConf = orbConf;
    this.orbTrackerMembers = orbTrackerMembers;
  }
  public Map<OrbTrackerMember,List<RawSplit>> assignInputSplits() {
    return null;
  }
  
  @Override
  public void setOrbConf(OrbConfiguration orbConf) {
    this.orbConf = orbConf;
  }
  
  @Override
  public OrbConfiguration getOrbConf() {
    return orbConf;
  }
}
