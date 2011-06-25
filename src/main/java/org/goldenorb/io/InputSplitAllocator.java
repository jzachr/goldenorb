package org.goldenorb.io;

import java.util.List;
import java.util.Map;

import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.io.input.RawSplit;

public class InputSplitAllocator<M> implements OrbConfigurable {
  
  private OrbConfiguration orbConf;
  
  public Map<M,List<RawSplit>> assignInputSplits() {
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
