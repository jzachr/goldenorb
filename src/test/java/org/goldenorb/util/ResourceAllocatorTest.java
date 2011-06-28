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
package org.goldenorb.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.InvalidJobConfException;
import org.goldenorb.OrbTracker;
import org.goldenorb.conf.OrbConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class ResourceAllocatorTest {
  Logger logger = LoggerFactory.getLogger(this.getClass());
  
  @Test
  public void testEnoughCapacityWithPPM() {
    List<OrbTracker> orbTrackers = new ArrayList<OrbTracker>();
    OrbConfiguration conf = new OrbConfiguration(true);
    conf.setOrbRequestedPartitions(6);
    conf.setOrbReservedPartitions(2);
    // the "PPM" part
    conf.setNumberOfPartitionsPerMachine(2);
    
    for(int i = 0; i < 4; i++) {
      OrbTracker ot = new OrbTracker(conf);
      ot.setAvailablePartitions(3);
      ot.setReservedPartitions(1);
      orbTrackers.add(ot);
    }
    
    ResourceAllocator<OrbTracker> ra = new ResourceAllocator<OrbTracker>(conf, orbTrackers);
    Map<OrbTracker,Integer[]> ret = null;
    try {
      ret = ra.assignResources(conf);
    } catch (InvalidJobConfException e) {
      e.printStackTrace();
    }
    
    // check each assignment
    assertEquals(ret.get(orbTrackers.get(0))[ResourceAllocator.TRACKER_AVAILABLE].intValue(), 2);
    assertEquals(ret.get(orbTrackers.get(0))[ResourceAllocator.TRACKER_RESERVED].intValue(), 1);
    assertEquals(ret.get(orbTrackers.get(1))[ResourceAllocator.TRACKER_AVAILABLE].intValue(), 2);
    assertEquals(ret.get(orbTrackers.get(1))[ResourceAllocator.TRACKER_RESERVED].intValue(), 1);
    assertEquals(ret.get(orbTrackers.get(2))[ResourceAllocator.TRACKER_AVAILABLE].intValue(), 2);
    assertEquals(ret.get(orbTrackers.get(2))[ResourceAllocator.TRACKER_RESERVED].intValue(), 0);
    assertEquals(ret.get(orbTrackers.get(3))[ResourceAllocator.TRACKER_AVAILABLE].intValue(), 0);
    assertEquals(ret.get(orbTrackers.get(3))[ResourceAllocator.TRACKER_RESERVED].intValue(), 0);
  }
  
  @Test
  public void testEnoughCapacity() {
    List<OrbTracker> orbTrackers = new ArrayList<OrbTracker>();
    OrbConfiguration conf = new OrbConfiguration(true);
    conf.setOrbRequestedPartitions(6);
    conf.setOrbReservedPartitions(2);
    conf.setNumberOfPartitionsPerMachine(0);
    
    for(int i = 0; i < 4; i++) {
      OrbTracker ot = new OrbTracker(conf);
      ot.setAvailablePartitions(3);
      ot.setReservedPartitions(1);
      orbTrackers.add(ot);
    }
    
    ResourceAllocator<OrbTracker> ra = new ResourceAllocator<OrbTracker>(conf, orbTrackers);
    Map<OrbTracker,Integer[]> ret = null;
    try {
      ret = ra.assignResources(conf);
    } catch (InvalidJobConfException e) {
      e.printStackTrace();
    }
    
    // check each assignment
    assertEquals(ret.get(orbTrackers.get(0))[ResourceAllocator.TRACKER_AVAILABLE].intValue(), 2);
    assertEquals(ret.get(orbTrackers.get(0))[ResourceAllocator.TRACKER_RESERVED].intValue(), 1);
    assertEquals(ret.get(orbTrackers.get(1))[ResourceAllocator.TRACKER_AVAILABLE].intValue(), 2);
    assertEquals(ret.get(orbTrackers.get(1))[ResourceAllocator.TRACKER_RESERVED].intValue(), 1);
    assertEquals(ret.get(orbTrackers.get(2))[ResourceAllocator.TRACKER_AVAILABLE].intValue(), 1);
    assertEquals(ret.get(orbTrackers.get(2))[ResourceAllocator.TRACKER_RESERVED].intValue(), 0);
    assertEquals(ret.get(orbTrackers.get(3))[ResourceAllocator.TRACKER_AVAILABLE].intValue(), 1);
    assertEquals(ret.get(orbTrackers.get(3))[ResourceAllocator.TRACKER_RESERVED].intValue(), 0);
  }
  
  @Test
  public void testUnbalancedAssignment() {
    List<OrbTracker> orbTrackers = new ArrayList<OrbTracker>();
    OrbConfiguration conf = new OrbConfiguration(true);
    conf.setOrbRequestedPartitions(6);
    conf.setOrbReservedPartitions(2);
    conf.setNumberOfPartitionsPerMachine(0);
    
    for(int i = 0; i < 4; i++) {
      OrbTracker ot = new OrbTracker(conf);
      ot.setAvailablePartitions(3);
      ot.setReservedPartitions(1);
      orbTrackers.add(ot);
    }
    
    orbTrackers.get(1).setAvailablePartitions(1);
    orbTrackers.get(2).setAvailablePartitions(1);
    
    ResourceAllocator<OrbTracker> ra = new ResourceAllocator<OrbTracker>(conf, orbTrackers);
    Map<OrbTracker,Integer[]> ret = null;
    try {
      ret = ra.assignResources(conf);
    } catch (InvalidJobConfException e) {
      e.printStackTrace();
    }

    // check each assignment
    assertEquals(ret.get(orbTrackers.get(0))[ResourceAllocator.TRACKER_AVAILABLE].intValue(), 2);
    assertEquals(ret.get(orbTrackers.get(0))[ResourceAllocator.TRACKER_RESERVED].intValue(), 1);
    
    assertEquals(ret.get(orbTrackers.get(1))[ResourceAllocator.TRACKER_AVAILABLE].intValue(), 1);
    assertEquals(ret.get(orbTrackers.get(1))[ResourceAllocator.TRACKER_RESERVED].intValue(), 1);
    
    assertEquals(ret.get(orbTrackers.get(2))[ResourceAllocator.TRACKER_AVAILABLE].intValue(), 1);
    assertEquals(ret.get(orbTrackers.get(2))[ResourceAllocator.TRACKER_RESERVED].intValue(), 0);
    
    assertEquals(ret.get(orbTrackers.get(3))[ResourceAllocator.TRACKER_AVAILABLE].intValue(), 2);
    assertEquals(ret.get(orbTrackers.get(3))[ResourceAllocator.TRACKER_RESERVED].intValue(), 0);
  }
  
/**
 * 
 */
  @Test
  public void insufficientCapacity() {
    List<OrbTracker> orbTrackers = new ArrayList<OrbTracker>();
    OrbConfiguration conf = new OrbConfiguration(true);
    conf.setOrbRequestedPartitions(6);
    conf.setOrbReservedPartitions(2);
    conf.setNumberOfPartitionsPerMachine(0);
    
    for(int i = 0; i < 4; i++) {
      OrbTracker ot = new OrbTracker(conf);
      ot.setAvailablePartitions(1);
      ot.setReservedPartitions(1);
      orbTrackers.add(ot);
    }
    
    ResourceAllocator<OrbTracker> ra = new ResourceAllocator<OrbTracker>(conf, orbTrackers);
    Map<OrbTracker,Integer[]> ret = null;
    try {
      ret = ra.assignResources(conf);
    } catch (InvalidJobConfException e) {
      e.printStackTrace();
    }
    assertNull(ret);
  }
}
