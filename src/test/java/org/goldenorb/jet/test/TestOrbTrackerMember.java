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

import org.goldenorb.jet.OrbTrackerMember;

public class TestOrbTrackerMember {
  
  private OrbTrackerMember orbTrackerMember;
  private OrbTrackerMember orbTrackerMemberOut;
  private static final int INT_PARTITIONCAPACITY_VALUE = 3;
  private static final int INT_AVAILABLEPARTITIONS_VALUE = 1;
  private static final int INT_RESERVEDPARTITIONS_VALUE = 4;
  private static final int INT_INUSEPARTITIONS_VALUE = 2;
  private static final String STRING_HOSTNAME_VALUE = "www.goldenorb.org";
  private static final boolean BOOLEAN_LEADER_VALUE = false;
  private static final int INT_PORT_VALUE = 3696;
  
  /*
   * Start of user / non-generated code -- any code written outside of this block will be
   * removed in subsequent code generations.
   */

  /* End of user / non-generated code */
  
  @Before 
  public void testOrbTrackerMember() throws IOException {
    /*
     * Start of user / non-generated code -- any code written outside of this block will be
     * removed in subsequent code generations.
     */

    /* End of user / non-generated code */
  
    orbTrackerMember = new OrbTrackerMember();
    orbTrackerMember.setPartitionCapacity(INT_PARTITIONCAPACITY_VALUE);
    orbTrackerMember.setAvailablePartitions(INT_AVAILABLEPARTITIONS_VALUE);
    orbTrackerMember.setReservedPartitions(INT_RESERVEDPARTITIONS_VALUE);
    orbTrackerMember.setInUsePartitions(INT_INUSEPARTITIONS_VALUE);
    orbTrackerMember.setHostname(STRING_HOSTNAME_VALUE);
    orbTrackerMember.setLeader(BOOLEAN_LEADER_VALUE);
    orbTrackerMember.setPort(INT_PORT_VALUE);
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    orbTrackerMember.write(out);
    DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    orbTrackerMemberOut = new OrbTrackerMember();
    orbTrackerMemberOut.readFields(in);
    
    /*
     * Start of user / non-generated code -- any code written outside of this block will be
     * removed in subsequent code generations.
     */

    /* End of user / non-generated code */
  }

  @Test 
  public void testPartitionCapacity() {
    assertEquals(orbTrackerMember.getPartitionCapacity(), orbTrackerMemberOut.getPartitionCapacity());
    assertEquals(orbTrackerMemberOut.getPartitionCapacity(), INT_PARTITIONCAPACITY_VALUE);
  }

  @Test 
  public void testAvailablePartitions() {
    assertEquals(orbTrackerMember.getAvailablePartitions(), orbTrackerMemberOut.getAvailablePartitions());
    assertEquals(orbTrackerMemberOut.getAvailablePartitions(), INT_AVAILABLEPARTITIONS_VALUE);
  }

  @Test 
  public void testReservedPartitions() {
    assertEquals(orbTrackerMember.getReservedPartitions(), orbTrackerMemberOut.getReservedPartitions());
    assertEquals(orbTrackerMemberOut.getReservedPartitions(), INT_RESERVEDPARTITIONS_VALUE);
  }

  @Test 
  public void testInUsePartitions() {
    assertEquals(orbTrackerMember.getInUsePartitions(), orbTrackerMemberOut.getInUsePartitions());
    assertEquals(orbTrackerMemberOut.getInUsePartitions(), INT_INUSEPARTITIONS_VALUE);
  }

  @Test 
  public void testHostname() {
    assertEquals(orbTrackerMember.getHostname(), orbTrackerMemberOut.getHostname());
    assertEquals(orbTrackerMemberOut.getHostname(), STRING_HOSTNAME_VALUE);
  }

  @Test 
  public void testLeader() {
    assertEquals(orbTrackerMember.isLeader(), orbTrackerMemberOut.isLeader());
    assertEquals(orbTrackerMemberOut.isLeader(), BOOLEAN_LEADER_VALUE);
  }

  @Test 
  public void testPort() {
    assertEquals(orbTrackerMember.getPort(), orbTrackerMemberOut.getPort());
    assertEquals(orbTrackerMemberOut.getPort(), INT_PORT_VALUE);
  }

  /*
   * Start of user / non-generated code -- any code written outside of this block will be
   * removed in subsequent code generations.
   */

  /* End of user / non-generated code */
}
