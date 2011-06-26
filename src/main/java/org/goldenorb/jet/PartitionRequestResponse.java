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
import org.apache.hadoop.io.Writable;

/**
 * This class is the response made to the Master by the OrbTracker slave.
 */
public class PartitionRequestResponse implements Writable {
  
  /**
   * the number of reserved partitions reserved to the job (SUCCESS) or the number available (NOT_AVAILABLE)
   */
  private int reservedPartitions;
  
  /**
   * the number of active partitions reserved to the job (SUCCESS) or the number available (NOT_AVAILABLE)
   */
  private int activePartitions;
  
  /**
   * the success criteria of the request attempt 0-SUCCESS, 1-NOT_AVAILABLE, 2-FAILURE
   */
  private int response;
  
  /*
   * Start of non-generated variable declaration code -- any code written outside of this block will be
   * removed in subsequent code generations.
   */

  /* End of non-generated variable declaraction code */

  /**
   * 
   */
  public PartitionRequestResponse() {}
  
  /*
   * Start of non-generated method code -- any code written outside of this block will be removed in
   * subsequent code generations.
   */
  
  /* End of non-generated method code */
  
  /**
   * gets the number of reserved partitions reserved to the job (SUCCESS) or the number available (NOT_AVAILABLE)
   * @return
   */
  public int getReservedPartitions() {
    return reservedPartitions;
  }
  
  /**
   * sets the number of reserved partitions reserved to the job (SUCCESS) or the number available (NOT_AVAILABLE)
   * @param reservedPartitions
   */
  public void setReservedPartitions(int reservedPartitions) {
    this.reservedPartitions = reservedPartitions;
  }
  
  /**
   * gets the number of active partitions reserved to the job (SUCCESS) or the number available (NOT_AVAILABLE)
   * @return
   */
  public int getActivePartitions() {
    return activePartitions;
  }
  
  /**
   * sets the number of active partitions reserved to the job (SUCCESS) or the number available (NOT_AVAILABLE)
   * @param activePartitions
   */
  public void setActivePartitions(int activePartitions) {
    this.activePartitions = activePartitions;
  }
  
  /**
   * gets the success criteria of the request attempt 0-SUCCESS, 1-NOT_AVAILABLE, 2-FAILURE
   * @return
   */
  public int getResponse() {
    return response;
  }
  
  /**
   * sets the success criteria of the request attempt 0-SUCCESS, 1-NOT_AVAILABLE, 2-FAILURE
   * @param response
   */
  public void setResponse(int response) {
    this.response = response;
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
    response = in.readInt();
  }
  
/**
 * 
 * @param  DataOutput out
 */
  public void write(DataOutput out) throws IOException {
    out.writeInt(reservedPartitions);
    out.writeInt(activePartitions);
    out.writeInt(response);
  }
  
}
