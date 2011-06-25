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
  public void readFields(DataInput in) throws IOException {
    reservedPartitions = in.readInt();
    activePartitions = in.readInt();
    jobID = Text.readString(in);
    basePartitionID = in.readInt();
  }
  
  public void write(DataOutput out) throws IOException {
    out.writeInt(reservedPartitions);
    out.writeInt(activePartitions);
    Text.writeString(out, jobID);
    out.writeInt(basePartitionID);
  }
  
}
