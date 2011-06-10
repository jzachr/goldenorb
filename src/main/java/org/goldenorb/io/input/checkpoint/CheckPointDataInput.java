package org.goldenorb.io.input.checkpoint;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.goldenorb.conf.OrbConfiguration;


public class CheckPointDataInput extends FSDataInputStream {
  
  
  public CheckPointDataInput(OrbConfiguration orbConf, int super_step, int partition) throws IOException {
    super((FileSystem.get(URI.create(orbConf.getFileOutputPath() + "/" + orbConf.getJobNumber() + "/" + super_step
                    + "/" + partition + "/SS" + super_step + "Part" + partition), orbConf) )
                    .open(new Path(orbConf.getFileOutputPath() + "/" + orbConf.getJobNumber() + "/" + super_step
                    + "/" + partition + "/SS" + super_step + "Part" + partition)));
  }

}
