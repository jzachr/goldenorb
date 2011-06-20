package org.goldenorb.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.InvalidJobConfException;
import org.goldenorb.OrbTracker;
import org.goldenorb.conf.OrbConfiguration;
import org.junit.Test;

import static org.junit.Assert.*;

public class ResourceAllocatorTest {
  
  @Test
  public void testEnoughCapacityWithPPM() {
    List<OrbTracker> orbTrackers = new ArrayList<OrbTracker>();
    OrbConfiguration conf = new OrbConfiguration(true);
    conf.setOrbRequestedPartitions("6");
    conf.setOrbReservedPartitions("2");
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
      ret = ra.assignResources();
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
    conf.setOrbRequestedPartitions("6");
    conf.setOrbReservedPartitions("2");
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
      ret = ra.assignResources();
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
    conf.setOrbRequestedPartitions("6");
    conf.setOrbReservedPartitions("2");
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
      ret = ra.assignResources();
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
  
  @Test
  public void insufficientCapacity() {
    List<OrbTracker> orbTrackers = new ArrayList<OrbTracker>();
    OrbConfiguration conf = new OrbConfiguration(true);
    conf.setOrbRequestedPartitions("6");
    conf.setOrbReservedPartitions("2");
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
      ret = ra.assignResources();
    } catch (InvalidJobConfException e) {
      e.printStackTrace();
    }
    assertNull(ret);
  }
}
