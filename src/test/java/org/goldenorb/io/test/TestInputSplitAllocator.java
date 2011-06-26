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
package org.goldenorb.io.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.io.InputSplitAllocator;
import org.goldenorb.io.input.RawSplit;
import org.goldenorb.jet.OrbPartitionMember;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestInputSplitAllocator {
  
  private Logger LOG;
  
/**
 * 
 */
  @Test
  public void inputSplitAllocatorTest(){
    
    LOG = LoggerFactory.getLogger(TestInputSplitAllocator.class);

    String[] rs1l = {"A", "B"};
    RawSplitWithID rs1 = new RawSplitWithID("rs1", rs1l);
    String[] rs2l = {"B", "C"};
    RawSplitWithID rs2 = new RawSplitWithID("rs2", rs2l);
    String[] rs3l = {"E", "F"};
    RawSplitWithID rs3 = new RawSplitWithID("rs3", rs3l);
    String[] rs4l = {"C"};
    RawSplitWithID rs4 = new RawSplitWithID("rs4", rs4l);
    List<RawSplit> rawSplits = new ArrayList<RawSplit>();
    
    rawSplits.add(rs1);
    rawSplits.add(rs2);
    rawSplits.add(rs3);
    rawSplits.add(rs4);
    
    OrbPartitionMember opm1 = new OrbPartitionMember();
    opm1.setHostname("A");
    opm1.setPort(0);
    OrbPartitionMember opm2 = new OrbPartitionMember();
    opm2.setHostname("A");
    opm2.setPort(1);
    OrbPartitionMember opm3 = new OrbPartitionMember();
    opm3.setHostname("B");
    opm3.setPort(0);
    OrbPartitionMember opm4 = new OrbPartitionMember();
    opm4.setHostname("B");
    opm4.setPort(1);
    OrbPartitionMember opm5 = new OrbPartitionMember();
    opm5.setHostname("C");
    opm5.setPort(0);
    OrbPartitionMember opm6 = new OrbPartitionMember();
    opm6.setHostname("C");
    opm6.setPort(1);

    List<OrbPartitionMember> orbPartitionMembers = new ArrayList<OrbPartitionMember>();
    orbPartitionMembers.add(opm1);
    orbPartitionMembers.add(opm2);
    orbPartitionMembers.add(opm3);
    orbPartitionMembers.add(opm4);
    orbPartitionMembers.add(opm5);
    orbPartitionMembers.add(opm6);
    OrbConfiguration orbConf = new OrbConfiguration();
    InputSplitAllocator isa = new InputSplitAllocator(orbConf, orbPartitionMembers);
    Map<OrbPartitionMember, List<RawSplit>> assignedSplits = isa.assignInputSplits(rawSplits);
    
    for(OrbPartitionMember orbPartitionMember: assignedSplits.keySet()){
      LOG.info(orbPartitionMember.getHostname() + ":" + orbPartitionMember.getPort() + " | " + assignedSplits.get(orbPartitionMember));
      assertTrue(assignedSplits.get(orbPartitionMember).size() < 2);
    }
  }
}
