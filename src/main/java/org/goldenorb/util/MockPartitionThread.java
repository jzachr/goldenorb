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
package org.goldenorb.util;

import java.io.FileOutputStream;

import org.goldenorb.PartitionProcess;
import org.goldenorb.conf.OrbConfiguration;

public class MockPartitionThread implements PartitionProcess, Runnable {
  
  private Thread thread;
  private int processNum;
  private boolean reserved = false;
  private OrbConfiguration orbConf;
  
/**
 * Constructor
 *
 */
  public MockPartitionThread() {
    thread = new Thread(this);
  }
  
/**
 * 
 */
  @Override
  public void run() {
    // TODO Auto-generated method stub
  }
  
/**
 * 
 * @param  FileOutputStream outStream
 * @param  FileOutputStream errStream
 */
  @Override
  public void launch(FileOutputStream outStream, FileOutputStream errStream) {
    thread.start();
  }
  
/**
 * 
 */
  @Override
  public void kill() {
    try {
      thread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
/**
 * Return the conf
 */
  @Override
  public OrbConfiguration getConf() {
    return orbConf;
  }
  
/**
 * Set the conf
 * @param  OrbConfiguration conf
 */
  @Override
  public void setConf(OrbConfiguration conf) {
    this.orbConf = conf;
  }
  
/**
 * Return the processNum
 */
  @Override
  public int getProcessNum() {
    return processNum;
  }
  
/**
 * Set the processNum
 * @param  int processNum
 */
  @Override
  public void setProcessNum(int processNum) {
    this.processNum = processNum;
  }
  
/**
 * Return the unning
 */
  @Override
  public boolean isRunning() {
    return thread.isAlive();
  }
  
/**
 * Set the reserved
 * @param  boolean reserved
 */
  @Override
  public void setReserved(boolean reserved) {
    this.reserved = reserved;
  }
  
/**
 * Return the eserved
 */
  @Override
  public boolean isReserved() {
    return reserved;
  }

/**
 * Set the partitionID
 * @param  int partitionID
 */
  @Override
  public void setPartitionID(int partitionID) {
    // TODO Auto-generated method stub
    
  }

/**
 * Return the partitionID
 */
  @Override
  public int getPartitionID() {
    // TODO Auto-generated method stub
    return 0;
  }
}
