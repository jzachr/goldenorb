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
 */

package org.goldenorb.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.io.input.RawSplit;
import org.goldenorb.jet.OrbPartitionMember;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InputSplitAllocator implements OrbConfigurable {
  
  private OrbConfiguration orbConf;
  private List<OrbPartitionMember> orbPartitionMembers;
  
  private Map<String, Map<Integer, List<RawSplit>>> hostToPortToRawSplits = new HashMap<String, Map<Integer, List<RawSplit>>>();
  private Map<String, Integer> hostToRawSplitCount = new HashMap<String, Integer>();
  
  private Logger logger;
  
  public InputSplitAllocator(OrbConfiguration orbConf, List<OrbPartitionMember> orbPartitionMembers) {
    this.orbConf = orbConf;
    this.orbPartitionMembers = orbPartitionMembers;
    logger = LoggerFactory.getLogger(InputSplitAllocator.class);
  }
  
  public Map<OrbPartitionMember,List<RawSplit>> assignInputSplits(List<RawSplit> rawSplits) {
    Map<OrbPartitionMember,List<RawSplit>> mapped = new HashMap<OrbPartitionMember,List<RawSplit>>();
    
    //creates a new HashMap<PortNumber, List<RawSplit>> for every host location if it doesn't exist
    for (RawSplit rSplit : rawSplits) {
      for (String rSplitLocation : rSplit.getLocations()) {
        if (!hostToPortToRawSplits.containsKey(rSplitLocation)) {
          hostToPortToRawSplits.put(rSplitLocation, new HashMap<Integer, List<RawSplit>>());
        }
      }
    }
    
    for (RawSplit rSplit : rawSplits) {
      try {
        System.out.println(getLightestHost(rSplit));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    /*
    String lightestHost = getLightestHost(rawSplit);
    int lightestPort = getLightestPort(lightestHost);
    List<RawSplit> rawSplits = hostToPortToRawSplits.get(lightestHost).get(lightestPort);
    rawSplits.add(rawSplit);
    hostToPortToRawSplits.get(lightestHost).put(lightestPort, rawSplits);
    */
    
    return mapped;
  }
  
  public String getLightestHost(RawSplit rawSplit) throws IOException {
    String smallestHost = "";
    int smallestInt = Integer.MAX_VALUE;
    
    for (String location : rawSplit.getLocations()) {
      logger.info("RawSplit returned locations: " + location);
      int locationCount = hostToRawSplitCount.get(location);
      if (locationCount < smallestInt) {
        smallestInt = locationCount;
        smallestHost = location;
      }
    }
    return smallestHost;
  }
  
  public int getLightestPort(String host) throws IOException {
    int smallestPort = 0;
    int smallestInt = Integer.MAX_VALUE;
    
    for (int port : hostToPortToRawSplits.get(host).keySet()) {
      logger.info("Available ports: " + port);
      int portCount = hostToPortToRawSplits.get(host).get(port).size();
      if (portCount < smallestInt) {
        smallestInt = portCount;
        smallestPort = port;
      }
    }
    return smallestPort;
  }
  
  @Override
  public void setOrbConf(OrbConfiguration orbConf) {
    this.orbConf = orbConf;
  }
  
  @Override
  public OrbConfiguration getOrbConf() {
    return orbConf;
  }
}
