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
package org.goldenorb.jet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*
 * Start of non-generated import declaration code -- any code written outside of this block will be
 * removed in subsequent code generations.
 */

/* End of non-generated import declaraction code */

/**
 * This class is a request made from the master OrbTracker to a slave OrbTracker to apply Partitions to a Job
 */
public class PartitionRequest implements Writable {
  
  /**
   * the number of reserved partitions requested
   */
  private int reservedPartitions;
  
  /**
   * the number of partitions requested to become active
   */
  private int activePartitions;
  
  /**
   * the jobID the partitions are requested for
   */
  private String jobID;
  
  /**
   * the base ID used by partition manager to launch partitions
   */
  private int basePartitionID;
  
  /*
   * Start of non-generated variable declaration code -- any code written outside of this block will be
   * removed in subsequent code generations.
   */

  /* End of non-generated variable declaraction code */

  /**
   * 
   */
  public PartitionRequest() {}
  
  /*
   * Start of non-generated method code -- any code written outside of this block will be removed in
   * subsequent code generations.
   */
  
  /* End of non-generated method code */
  @Override
  public String toString(){
    return getReservedPartitions() + " " + getActivePartitions() + " " + getJobID() + " " + getBasePartitionID();
  }
  /**
   * gets the number of reserved partitions requested
   * @return
   */
  public int getReservedPartitions() {
    return reservedPartitions;
  }
  
  /**
   * sets the number of reserved partitions requested
   * @param reservedPartitions
   */
  public void setReservedPartitions(int reservedPartitions) {
    this.reservedPartitions = reservedPartitions;
  }
  
  /**
   * gets the number of partitions requested to become active
   * @return
   */
  public int getActivePartitions() {
    return activePartitions;
  }
  
  /**
   * sets the number of partitions requested to become active
   * @param activePartitions
   */
  public void setActivePartitions(int activePartitions) {
    this.activePartitions = activePartitions;
  }
  
  /**
   * gets the jobID the partitions are requested for
   * @return
   */
  public String getJobID() {
    return jobID;
  }
  
  /**
   * sets the jobID the partitions are requested for
   * @param jobID
   */
  public void setJobID(String jobID) {
    this.jobID = jobID;
  }
  
  /**
   * gets the base ID used by partition manager to launch partitions
   * @return
   */
  public int getBasePartitionID() {
    return basePartitionID;
  }
  
  /**
   * sets the base ID used by partition manager to launch partitions
   * @param basePartitionID
   */
  public void setBasePartitionID(int basePartitionID) {
    this.basePartitionID = basePartitionID;
  }
  
  
  // /////////////////////////////////////
  // Writable
  // /////////////////////////////////////
/**
 * 
 * @param  DataInput in
 */
  public void readFields(DataInput in) throws IOException {
    reservedPartitions = in.readInt();
    activePartitions = in.readInt();
    jobID = Text.readString(in);
    basePartitionID = in.readInt();
  }
  
/**
 * 
 * @param  DataOutput out
 */
  public void write(DataOutput out) throws IOException {
    out.writeInt(reservedPartitions);
    out.writeInt(activePartitions);
    Text.writeString(out, jobID);
    out.writeInt(basePartitionID);
  }
  
}
