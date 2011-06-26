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

import org.goldenorb.jet.OrbPartitionMember;

public class TestOrbPartitionMember {
  
  private OrbPartitionMember orbPartitionMember;
  private OrbPartitionMember orbPartitionMemberOut;
  private static final int INT_PARTITIONID_VALUE = 1;
  private static final int INT_NUMBEROFVERTICES_VALUE = 10000;
  private static final int INT_SUPERSTEP_VALUE = 4;
  private static final int INT_MESSAGESSENT_VALUE = 10000;
  private static final float FLOAT_PERCENTCOMPLETE_VALUE = 1;
  private static final String STRING_HOSTNAME_VALUE = "www.goldenorb.org";
  private static final boolean BOOLEAN_LEADER_VALUE = false;
  private static final int INT_PORT_VALUE = 3696;
  
  /*
   * Start of user / non-generated code -- any code written outside of this block will be
   * removed in subsequent code generations.
   */

  /* End of user / non-generated code */
  
  @Before 
  public void testOrbPartitionMember() throws IOException {
    /*
     * Start of user / non-generated code -- any code written outside of this block will be
     * removed in subsequent code generations.
     */

    /* End of user / non-generated code */
  
    orbPartitionMember = new OrbPartitionMember();
    orbPartitionMember.setPartitionID(INT_PARTITIONID_VALUE);
    orbPartitionMember.setNumberOfVertices(INT_NUMBEROFVERTICES_VALUE);
    orbPartitionMember.setSuperStep(INT_SUPERSTEP_VALUE);
    orbPartitionMember.setMessagesSent(INT_MESSAGESSENT_VALUE);
    orbPartitionMember.setPercentComplete(FLOAT_PERCENTCOMPLETE_VALUE);
    orbPartitionMember.setHostname(STRING_HOSTNAME_VALUE);
    orbPartitionMember.setLeader(BOOLEAN_LEADER_VALUE);
    orbPartitionMember.setPort(INT_PORT_VALUE);
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    orbPartitionMember.write(out);
    DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    orbPartitionMemberOut = new OrbPartitionMember();
    orbPartitionMemberOut.readFields(in);
    
    /*
     * Start of user / non-generated code -- any code written outside of this block will be
     * removed in subsequent code generations.
     */

    /* End of user / non-generated code */
  }

  @Test 
  public void testPartitionID() {
    assertEquals(orbPartitionMember.getPartitionID(), orbPartitionMemberOut.getPartitionID());
    assertEquals(orbPartitionMemberOut.getPartitionID(), INT_PARTITIONID_VALUE);
  }

  @Test 
  public void testNumberOfVertices() {
    assertEquals(orbPartitionMember.getNumberOfVertices(), orbPartitionMemberOut.getNumberOfVertices());
    assertEquals(orbPartitionMemberOut.getNumberOfVertices(), INT_NUMBEROFVERTICES_VALUE);
  }

  @Test 
  public void testSuperStep() {
    assertEquals(orbPartitionMember.getSuperStep(), orbPartitionMemberOut.getSuperStep());
    assertEquals(orbPartitionMemberOut.getSuperStep(), INT_SUPERSTEP_VALUE);
  }

  @Test 
  public void testMessagesSent() {
    assertEquals(orbPartitionMember.getMessagesSent(), orbPartitionMemberOut.getMessagesSent());
    assertEquals(orbPartitionMemberOut.getMessagesSent(), INT_MESSAGESSENT_VALUE);
  }

  @Test 
  public void testPercentComplete() {
    assertEquals(orbPartitionMember.getPercentComplete(), orbPartitionMemberOut.getPercentComplete(), FLOAT_PERCENTCOMPLETE_VALUE*.10 );
    assertEquals(orbPartitionMemberOut.getPercentComplete(), FLOAT_PERCENTCOMPLETE_VALUE, FLOAT_PERCENTCOMPLETE_VALUE*.10);
  }

  @Test 
  public void testHostname() {
    assertEquals(orbPartitionMember.getHostname(), orbPartitionMemberOut.getHostname());
    assertEquals(orbPartitionMemberOut.getHostname(), STRING_HOSTNAME_VALUE);
  }

  @Test 
  public void testLeader() {
    assertEquals(orbPartitionMember.isLeader(), orbPartitionMemberOut.isLeader());
    assertEquals(orbPartitionMemberOut.isLeader(), BOOLEAN_LEADER_VALUE);
  }

  @Test 
  public void testPort() {
    assertEquals(orbPartitionMember.getPort(), orbPartitionMemberOut.getPort());
    assertEquals(orbPartitionMemberOut.getPort(), INT_PORT_VALUE);
  }

  /*
   * Start of user / non-generated code -- any code written outside of this block will be
   * removed in subsequent code generations.
   */

  /* End of user / non-generated code */
}
