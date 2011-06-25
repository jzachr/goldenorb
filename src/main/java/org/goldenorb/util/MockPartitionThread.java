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
  
  public MockPartitionThread() {
    thread = new Thread(this);
  }
  
  @Override
  public void run() {
    // TODO Auto-generated method stub
  }
  
  @Override
  public void launch(FileOutputStream outStream, FileOutputStream errStream) {
    thread.start();
  }
  
  @Override
  public void kill() {
    try {
      thread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public OrbConfiguration getConf() {
    return orbConf;
  }
  
  @Override
  public void setConf(OrbConfiguration conf) {
    this.orbConf = conf;
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
    return thread.isAlive();
  }
  
  @Override
  public void setReserved(boolean reserved) {
    this.reserved = reserved;
  }
  
  @Override
  public boolean isReserved() {
    return reserved;
  }
}
