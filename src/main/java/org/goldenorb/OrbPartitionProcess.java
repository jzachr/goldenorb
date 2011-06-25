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
 */
package org.goldenorb;

import java.io.FileOutputStream;
import java.io.IOException;

import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.util.StreamWriter;

public class OrbPartitionProcess implements PartitionProcess {
  private Process process;
  private OrbConfiguration conf;
  private int processNum;
  private boolean reserved = false;
  private int partitionID;
  
  public OrbPartitionProcess() {}
  
  public OrbPartitionProcess(OrbConfiguration conf, int processNum, boolean reserved, int partitionID) {
    this.conf = conf;
    this.processNum = processNum;
    this.reserved = reserved;
    this.partitionID = partitionID;
  }
  
  @Override
  public void launch(FileOutputStream outStream, FileOutputStream errStream) {
    // TODO Need to update Process launch arguments once OrbPartition is completed
    try {
      ProcessBuilder builder = new ProcessBuilder("java", conf.getOrbPartitionJavaopts(), "-cp",
          "goldenorb-0.0.1-SNAPSHOT-jar-with-dependencies.jar" + buildClassPathPart(),
          "org.goldenorb.OrbPartition", conf.getOrbJobName().toString(), Integer.toString(partitionID),
          Boolean.toString(reserved), Integer.toString(conf.getOrbBasePort() + processNum));
      
      process = builder.start();
      new StreamWriter(process.getErrorStream(), errStream);
      new StreamWriter(process.getInputStream(), outStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  private String buildClassPathPart() {
    StringBuilder sb = new StringBuilder();
    for (String cp : conf.getOrbClassPaths()) {
      sb.append(":");
      sb.append(cp);
    }
    return sb.toString();
  }
  
  @Override
  public void kill() {
    process.destroy();
  }

  @Override
  public OrbConfiguration getConf() {
    return conf;
  }

  @Override
  public void setConf(OrbConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public int getProcessNum() {
    return processNum;
  }

  @Override
  public void setProcessNum(int processNum) {
    this.processNum = processNum;
  }

  @Override
  public boolean isRunning() {
    boolean ret = false;
    try {
      process.exitValue();
    } catch(IllegalThreadStateException e) {
      e.printStackTrace();
      ret = true;
    }
    
    return ret;
  }

  @Override
  public void setReserved(boolean reserved) {
    this.reserved = reserved;
  }

  @Override
  public boolean isReserved() {
    return reserved;
  }

  @Override
  public void setPartitionID(int partitionID) {
    this.partitionID = partitionID;
  }

  @Override
  public int getPartitionID() {
    return partitionID;
  }
}
