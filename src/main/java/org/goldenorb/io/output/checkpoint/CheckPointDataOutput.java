package org.goldenorb.io.output.checkpoint;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.goldenorb.conf.OrbConfiguration;

public class CheckPointDataOutput extends FSDataOutputStream {
  
  public CheckPointDataOutput(OrbConfiguration orbConf, int superStep, int partition) throws IOException {
    super(FileSystem.get(URI.create(orbConf.getFileOutputPath() + "/" + orbConf.getJobNumber() + "/" + superStep
          + "/" + partition + "/SS" + superStep + "Part" + partition), orbConf)
          .create(new Path(orbConf.getFileOutputPath() + "/" + orbConf.getJobNumber() + "/" + superStep
          + "/" + partition + "/SS" + superStep + "Part" + partition), true),
          FileSystem.getStatistics((orbConf.getFileOutputPath() + "/" + orbConf.getJobNumber() + "/" + superStep
          + "/" + partition + "/SS" + superStep + "Part" + partition), FileSystem.class));
  }
}
