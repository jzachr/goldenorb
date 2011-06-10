package org.goldenorb.jet.test;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import org.goldenorb.jet.PartitionRequest;

public class TestPartitionRequest {
  
  private PartitionRequest partitionRequest;
  private PartitionRequest partitionRequestOut;
  private static final int INT_RESERVEDPARTITIONS_VALUE = 1;
  private static final int INT_ACTIVEPARTITIONS_VALUE = 2;
  private static final String STRING_JOBID_VALUE = "job102345678";
  
  /*
   * Start of user / non-generated code -- any code written outside of this block will be
   * removed in subsequent code generations.
   */

  /* End of user / non-generated code */
  
  @Before 
  public void testPartitionRequest() throws IOException {
    /*
     * Start of user / non-generated code -- any code written outside of this block will be
     * removed in subsequent code generations.
     */

    /* End of user / non-generated code */
  
    partitionRequest = new PartitionRequest();
    partitionRequest.setReservedPartitions(INT_RESERVEDPARTITIONS_VALUE);
    partitionRequest.setActivePartitions(INT_ACTIVEPARTITIONS_VALUE);
    partitionRequest.setJobID(STRING_JOBID_VALUE);
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    partitionRequest.write(out);
    DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    partitionRequestOut = new PartitionRequest();
    partitionRequestOut.readFields(in);
    
    /*
     * Start of user / non-generated code -- any code written outside of this block will be
     * removed in subsequent code generations.
     */

    /* End of user / non-generated code */
  }

  @Test 
  public void testReservedPartitions() {
    assertEquals(partitionRequest.getReservedPartitions(), partitionRequestOut.getReservedPartitions());
    assertEquals(partitionRequestOut.getReservedPartitions(), INT_RESERVEDPARTITIONS_VALUE);
  }

  @Test 
  public void testActivePartitions() {
    assertEquals(partitionRequest.getActivePartitions(), partitionRequestOut.getActivePartitions());
    assertEquals(partitionRequestOut.getActivePartitions(), INT_ACTIVEPARTITIONS_VALUE);
  }

  @Test 
  public void testJobID() {
    assertEquals(partitionRequest.getJobID(), partitionRequestOut.getJobID());
    assertEquals(partitionRequestOut.getJobID(), STRING_JOBID_VALUE);
  }

  /*
   * Start of user / non-generated code -- any code written outside of this block will be
   * removed in subsequent code generations.
   */

  /* End of user / non-generated code */
}
