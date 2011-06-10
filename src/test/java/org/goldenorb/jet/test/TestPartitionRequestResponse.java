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

import org.goldenorb.jet.PartitionRequestResponse;

public class TestPartitionRequestResponse {
  
  private PartitionRequestResponse partitionRequestResponse;
  private PartitionRequestResponse partitionRequestResponseOut;
  private static final int INT_RESERVEDPARTITIONS_VALUE = 1;
  private static final int INT_ACTIVEPARTITIONS_VALUE = 2;
  private static final int INT_RESPONSE_VALUE = 3;
  
  /*
   * Start of user / non-generated code -- any code written outside of this block will be
   * removed in subsequent code generations.
   */

  /* End of user / non-generated code */
  
  @Before 
  public void testPartitionRequestResponse() throws IOException {
    /*
     * Start of user / non-generated code -- any code written outside of this block will be
     * removed in subsequent code generations.
     */

    /* End of user / non-generated code */
  
    partitionRequestResponse = new PartitionRequestResponse();
    partitionRequestResponse.setReservedPartitions(INT_RESERVEDPARTITIONS_VALUE);
    partitionRequestResponse.setActivePartitions(INT_ACTIVEPARTITIONS_VALUE);
    partitionRequestResponse.setResponse(INT_RESPONSE_VALUE);
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    partitionRequestResponse.write(out);
    DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    partitionRequestResponseOut = new PartitionRequestResponse();
    partitionRequestResponseOut.readFields(in);
    
    /*
     * Start of user / non-generated code -- any code written outside of this block will be
     * removed in subsequent code generations.
     */

    /* End of user / non-generated code */
  }

  @Test 
  public void testReservedPartitions() {
    assertEquals(partitionRequestResponse.getReservedPartitions(), partitionRequestResponseOut.getReservedPartitions());
    assertEquals(partitionRequestResponseOut.getReservedPartitions(), INT_RESERVEDPARTITIONS_VALUE);
  }

  @Test 
  public void testActivePartitions() {
    assertEquals(partitionRequestResponse.getActivePartitions(), partitionRequestResponseOut.getActivePartitions());
    assertEquals(partitionRequestResponseOut.getActivePartitions(), INT_ACTIVEPARTITIONS_VALUE);
  }

  @Test 
  public void testResponse() {
    assertEquals(partitionRequestResponse.getResponse(), partitionRequestResponseOut.getResponse());
    assertEquals(partitionRequestResponseOut.getResponse(), INT_RESPONSE_VALUE);
  }

  /*
   * Start of user / non-generated code -- any code written outside of this block will be
   * removed in subsequent code generations.
   */

  /* End of user / non-generated code */
}
