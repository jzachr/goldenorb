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
  public void readFields(DataInput in) throws IOException {
    reservedPartitions = in.readInt();
    activePartitions = in.readInt();
    response = in.readInt();
  }
  
  public void write(DataOutput out) throws IOException {
    out.writeInt(reservedPartitions);
    out.writeInt(activePartitions);
    out.writeInt(response);
  }
  
}
