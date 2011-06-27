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
import java.io.OutputStream;

import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.util.MockPartitionThread;

/**
 * {@link PartitionProcess} is a proxy object used when spawning {@link OrbPartition} processes 
 * (or {@link MockPartitionThread} threads) from {@link OrbPartitionManager}. 
 */
public interface PartitionProcess {
/**
 * 
 * @param  FileOutputStream outStream
 * @param  FileOutputStream errStream
 */
  public void launch(OutputStream outStream, OutputStream errStream);
/**
 * 
 */
  public void kill();
/**
 * Set the reserved
 * @param  boolean reserved
 */
  public void setReserved(boolean reserved);
/**
 * Return the eserved
 */
  public boolean isReserved();
/**
 * Return the conf
 */
  public OrbConfiguration getConf();
/**
 * Set the conf
 * @param  OrbConfiguration conf
 */
  public void setConf(OrbConfiguration conf);
/**
 * Return the processNum
 */
  public int getProcessNum();
/**
 * Set the processNum
 * @param  int processNum
 */
  public void setProcessNum(int processNum);
/**
 * Return the unning
 */
  public boolean isRunning();
/**
 * Set the partitionID
 * @param  int partitionID
 */
  public void setPartitionID(int partitionID);
/**
 * Return the partitionID
 */
  public int getPartitionID();
  
  public void setJobNumber(String jobNumber);

  public String getJobNumber();
}