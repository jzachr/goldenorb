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
package org.goldenorb.io.input.checkpoint;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.goldenorb.conf.OrbConfiguration;

/**
 * A Utility that sets a FSDataInputStream to read from a specific path on the HDFS to retrieve the data that was
 * backed up at a specific super step that was check pointed.
 * 
 * 
 */

public class CheckPointDataInput extends FSDataInputStream {
  
  
/**
 * Constructor - 
 * 
 * Ex. If you wanted to get a CheckPointDataInput set to read the data that was on partion 3 at super step 5
 * when it was check pointed.
 * 
 * <code>
 * CheckPointDataInput cpdiP3SS5 = new CheckPointDataInput(orbConf, super_super, step);
 * </code>
 *
 * The 
 *
 * @param  OrbConfiguration orbConf - OrbConfiguration that contains the FileOutputPath and job number
 *  that you want to connect to.
 * @param  int super_step - A previous super step you to retrieve data from.
 * @param  int partition - A partition to retrieve data that was previously written.
 */
  public CheckPointDataInput(OrbConfiguration orbConf, int super_step, int partition) throws IOException {
    super((FileSystem.get(URI.create(orbConf.getFileOutputPath() + "/" + orbConf.getJobNumber() + "/" + super_step
                    + "/" + partition + "/SS" + super_step + "Part" + partition), orbConf) )
                    .open(new Path(orbConf.getFileOutputPath() + "/" + orbConf.getJobNumber() + "/" + super_step
                    + "/" + partition + "/SS" + super_step + "Part" + partition)));
  }

}
