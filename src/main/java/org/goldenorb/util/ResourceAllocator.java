package org.goldenorb.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.mapred.InvalidJobConfException;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.jet.OrbTrackerMember;

public class ResourceAllocator<M extends OrbTrackerMember> {
  final Logger logger = LoggerFactory.getLogger(this.getClass());
  
  public static final int TRACKER_AVAILABLE = 0;
  public static final int TRACKER_RESERVED = 1;
  
  private OrbConfiguration conf;
  private Collection<M> orbTrackers;
  private List<M> trackersByAvailable;
  private List<M> trackersByReserved;
  private int totalAvailable = 0;
  private int totalReserved = 0;
  private Map<M,Integer[]> orbTrackerAssignments = new HashMap<M,Integer[]>();
  private Map<M,Integer[]> orbTrackerAvailability = new HashMap<M,Integer[]>();
  
  public ResourceAllocator(OrbConfiguration conf, Collection<M> orbTrackers) {
    this.setConf(conf);
    this.setOrbTrackers(orbTrackers);
  }
  
  public Map<M,Integer[]> assignResources() throws InvalidJobConfException {
    logger.info("ResourceAllocator: assignResources()");
    
    int requestedPartitions = conf.getOrbRequestedPartitions();
    // if no partitions are requested, the job is invalid
    if (requestedPartitions <= 0) {
      logger.error("missing number of requested partitions for job");
      throw new InvalidJobConfException("missing number of requested partitions for job");
    }
    int reservedPartitions = conf.getOrbReservedPartitions();
    
    // if this is zero, it screws up the resource allocation so we set it to Int max to move through
    // the allocation process
    int partitionsPerMachine = (conf.getNumberOfPartitionsPerMachine() == 0 ? Integer.MAX_VALUE : conf
        .getNumberOfPartitionsPerMachine());
    
    logger.debug("requestedPartitions: {}", requestedPartitions);
    logger.debug("reservedPartitions: {}", reservedPartitions);
    logger.debug("partitionsPerMachine: {}", partitionsPerMachine);
    
    // some setup, organize the trackers and capture availability/reserved info
    sortTrackers();
    updateOrbTrackerAvailability();
    
    // if you request more than what's available, you get nothing (caller should wait and try again)
    if (requestedPartitions > totalAvailable || reservedPartitions > totalReserved) {
      return null;
    }
    
    // requestedPartitions functions as the countdown mechanism
    while (requestedPartitions > 0) {
      // until all partitions are assigned, iterate through trackers
      for (Iterator<M> iter = trackersByAvailable.iterator(); iter.hasNext() && requestedPartitions > 0;) {
        M tracker = iter.next();
        // tracker has > partitionsPerMachine available
        if (partitionsPerMachine <= orbTrackerAvailability.get(tracker)[TRACKER_AVAILABLE]
            && requestedPartitions >= partitionsPerMachine) {
          assignAvailable(tracker, partitionsPerMachine, TRACKER_AVAILABLE);
          requestedPartitions -= partitionsPerMachine;
          decrementAvailable(tracker, partitionsPerMachine, TRACKER_AVAILABLE);
        }
        // tracker has at least one available
        else if (orbTrackerAvailability.get(tracker)[TRACKER_AVAILABLE] > 0) {
          assignAvailable(tracker, 1, TRACKER_AVAILABLE);
          requestedPartitions -= 1;
          decrementAvailable(tracker, 1, TRACKER_AVAILABLE);
        }
        // tracker is full, skip it
        else {
          continue;
        }
      }
    }
    
    // requestedPartitions functions as the countdown mechanism
    while (reservedPartitions > 0) {
      // until all partitions are assigned, iterate through trackers
      for (Iterator<M> iter = trackersByReserved.iterator(); iter.hasNext() && reservedPartitions > 0;) {
        M tracker = iter.next();
        // tracker has at least one available
        if (orbTrackerAvailability.get(tracker)[TRACKER_RESERVED] > 0) {
          assignAvailable(tracker, 1, TRACKER_RESERVED);
          reservedPartitions -= 1;
          decrementAvailable(tracker, 1, TRACKER_RESERVED);
        }
        // tracker is full, skip it
        else {
          continue;
        }
      }
    }
    
    return orbTrackerAssignments;
  }
  
  private void assignAvailable(M tracker, int count, int field) {
    Integer[] v = orbTrackerAssignments.get(tracker);
    v[field] += count;
    orbTrackerAssignments.put(tracker, v);
  }
  
  private void decrementAvailable(M tracker, int count, int field) {
    Integer[] v = orbTrackerAvailability.get(tracker);
    v[field] -= count;
    orbTrackerAvailability.put(tracker, v);
  }
  
  private void sortTrackers() {
    trackersByAvailable = new ArrayList<M>(getOrbTrackers());
    Collections.sort(trackersByAvailable, new Comparator<M>() {
      @Override
      public int compare(M o1, M o2) {
        Integer a1 = o1.getAvailablePartitions();
        Integer a2 = o2.getAvailablePartitions();
        return a1.compareTo(a2);
      }
    });
    trackersByReserved = new ArrayList<M>(getOrbTrackers());
    Collections.sort(trackersByReserved, new Comparator<M>() {
      @Override
      public int compare(M o1, M o2) {
        Integer a1 = o1.getReservedPartitions();
        Integer a2 = o2.getReservedPartitions();
        return a1.compareTo(a2);
      }
    });
  }
  
  private void updateOrbTrackerAvailability() {
    totalAvailable = 0;
    totalReserved = 0;
    for (M tracker : getOrbTrackers()) {
      int reserved = tracker.getReservedPartitions();
      int available = tracker.getAvailablePartitions();
      
      Integer[] v = new Integer[2];
      v[TRACKER_AVAILABLE] = available;
      v[TRACKER_RESERVED] = reserved;
      orbTrackerAvailability.put(tracker, v);
      
      Integer[] empty = {0, 0};
      orbTrackerAssignments.put(tracker, empty);
      
      totalReserved += reserved;
      totalAvailable += available;
    }
  }
  
  public void setConf(OrbConfiguration conf) {
    this.conf = conf;
  }
  
  public OrbConfiguration getConf() {
    return conf;
  }
  
  public Collection<M> getOrbTrackers() {
    return orbTrackers;
  }
  
  public void setOrbTrackers(Collection<M> orbTrackers) {
    this.orbTrackers = orbTrackers;
  }
}
