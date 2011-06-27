/**
 * Licensed to Ravel, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Ravel, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package org.goldenorb.io.output.checkpoint;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.goldenorb.conf.OrbConfiguration;

/**
 * A utility that sets a FSDataOutputStream that gets set to write to a specific path on the HDFS to back up the 
 * data of a partition at a check pointed super step.
 * 
 * 
 */

public class CheckPointDataOutput extends FSDataOutputStream {
  
/**
 * Constructor -
 *  
 *  Ex. If you wanted to get an CheckPointDataOutput to set to write the data that was on partition 3 at super step 5.
 * 
 * <code>
 * CheckPointDataInput cpdiP3SS5 = new CheckPointDataInput(orbConf, super_super, step);
 * </code>
 * 
 * This would write to the file out put path at JobNumberOfThisJob/5/3/SS5Part3 . SSPart3 is the file to be written to.
 * If the path and file do not exist, they are created.
 *
 *
 * @param  OrbConfiguration orbConf - OrbConfiguration that contains the FileOutputPath and job number
 *  that you want to connect to.
 * @param  int superStep - The super step at which this check point is being performed
 * @param  int partition - The partition who's data is getting backed up
 */
  public CheckPointDataOutput(OrbConfiguration orbConf, int superStep, int partition) throws IOException {
    super(FileSystem.get(URI.create(orbConf.getFileOutputPath() + "/" + orbConf.getJobNumber() + "/" + superStep
          + "/" + partition + "/SS" + superStep + "Part" + partition), orbConf)
          .create(new Path(orbConf.getFileOutputPath() + "/" + orbConf.getJobNumber() + "/" + superStep
          + "/" + partition + "/SS" + superStep + "Part" + partition), true),
          FileSystem.getStatistics((orbConf.getFileOutputPath() + "/" + orbConf.getJobNumber() + "/" + superStep
          + "/" + partition + "/SS" + superStep + "Part" + partition), FileSystem.class));
  }
}
