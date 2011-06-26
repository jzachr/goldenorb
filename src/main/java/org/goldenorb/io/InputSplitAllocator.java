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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.ReflectionUtils;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.io.input.RawSplit;
import org.goldenorb.jet.OrbPartitionMember;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
public class InputSplitAllocator implements OrbConfigurable {
  
  private OrbConfiguration orbConf;
  private List<OrbPartitionMember> orbPartitionMembers;
  
  private Map<String,Map<Integer,List<RawSplit>>> hostToPortToRawSplits = new HashMap<String,Map<Integer,List<RawSplit>>>();
  private Map<String,Integer> hostToRawSplitCount = new HashMap<String,Integer>();
  
  private Logger LOG;
  
  public InputSplitAllocator(OrbConfiguration orbConf, List<OrbPartitionMember> orbPartitionMembers) {
    this.orbConf = orbConf;
    this.orbPartitionMembers = orbPartitionMembers;
    LOG = LoggerFactory.getLogger(InputSplitAllocator.class);
    
    // initialize maps for placing rawSplits
    for (OrbPartitionMember orbPartitionMember : orbPartitionMembers) {
      if (!hostToRawSplitCount.containsKey(orbPartitionMember.getHostname())) {
        hostToRawSplitCount.put(orbPartitionMember.getHostname(), 0);
      }
      if (!hostToPortToRawSplits.containsKey(orbPartitionMember.getHostname())) {
        hostToPortToRawSplits.put(orbPartitionMember.getHostname(), new HashMap<Integer,List<RawSplit>>());
      }
      Map<Integer,List<RawSplit>> portMap = hostToPortToRawSplits.get(orbPartitionMember.getHostname());
      if (!portMap.containsKey(orbPartitionMember.getPort())) {
        portMap.put(orbPartitionMember.getPort(), new ArrayList<RawSplit>());
      }
    }
  }
  
  @SuppressWarnings({"deprecation", "rawtypes", "unchecked"})
  public Map<OrbPartitionMember,List<RawSplit>> assignInputSplits() {
    List<RawSplit> rawSplits = null;
    JobConf job = new JobConf(orbConf);
    JobContext jobContext = new JobContext(job, new JobID(orbConf.getOrbJobName(), 0));
    org.apache.hadoop.mapreduce.InputFormat<?,?> input;
    try {
      input = ReflectionUtils.newInstance(
        jobContext.getInputFormatClass(), orbConf);
    
    List<org.apache.hadoop.mapreduce.InputSplit> splits = input.getSplits(jobContext);
    rawSplits = new ArrayList<RawSplit>(splits.size());
    DataOutputBuffer buffer = new DataOutputBuffer();
    SerializationFactory factory = new SerializationFactory(orbConf);
    Serializer serializer = factory.getSerializer(splits.get(0).getClass());
    serializer.open(buffer);
    for (int i = 0; i < splits.size(); i++) {
      buffer.reset();
      serializer.serialize(splits.get(i));
      RawSplit rawSplit = new RawSplit();
      rawSplit.setClassName(splits.get(i).getClass().getName());
      rawSplit.setDataLength(splits.get(i).getLength());
      rawSplit.setBytes(buffer.getData(), 0, buffer.getLength());
      rawSplit.setLocations(splits.get(i).getLocations());
      rawSplits.add(rawSplit);
    }
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return assignInputSplits(rawSplits);
  }
  
  public Map<OrbPartitionMember,List<RawSplit>> assignInputSplits(Collection<RawSplit> rawSplits) {
    
    Map<OrbPartitionMember,List<RawSplit>> mapOfSplitsToPartitions = new HashMap<OrbPartitionMember,List<RawSplit>>();
    List<RawSplit> notLocalRawSplits = new ArrayList<RawSplit>();
    
    for (RawSplit rawSplit : rawSplits) {
      List<String> viableLocations = getViableHosts(rawSplit.getLocations());
      if (viableLocations.size() < 1) {
        notLocalRawSplits.add(rawSplit);
        LOG.debug("Non-local split found: " + rawSplit);
      } else {
        String host = getLightestHost(viableLocations);
        int port = getLightestPort(host);
        List<RawSplit> rawSplitAssignedList = hostToPortToRawSplits.get(host).get(port);
        rawSplitAssignedList.add(rawSplit);
        hostToPortToRawSplits.get(host).put(port, rawSplitAssignedList);
        int count = hostToRawSplitCount.get(host);
        count++;
        hostToRawSplitCount.put(host, count);
      }
    }
    
    for (RawSplit rawSplit : notLocalRawSplits) {
      String host = getLightestHostAll();
      int port = getLightestPort(host);
      List<RawSplit> rawSplitAssignedList = hostToPortToRawSplits.get(host).get(port);
      rawSplitAssignedList.add(rawSplit);
      hostToPortToRawSplits.get(host).put(port, rawSplitAssignedList);
      int count = hostToRawSplitCount.get(host);
      count++;
      hostToRawSplitCount.put(host, count);
    }
    
    for (OrbPartitionMember orbPartitionMember : orbPartitionMembers) {
      mapOfSplitsToPartitions.put(orbPartitionMember,
        hostToPortToRawSplits.get(orbPartitionMember.getHostname()).get(orbPartitionMember.getPort()));
    }
    
    return mapOfSplitsToPartitions;
  }
  
  private List<String> getViableHosts(String[] hosts) {
    List<String> viableHosts = new ArrayList<String>();
    for (String host : hosts) {
      if (hostToRawSplitCount.containsKey(host)) {
        viableHosts.add(host);
      }
    }
    return viableHosts;
  }
  
  private String getLightestHost(List<String> hosts) {
    String lightestHost = null;
    int lightestCount = Integer.MAX_VALUE;
    for (String host : hosts) {
      int hostCount = hostToRawSplitCount.get(host);
      if (hostCount < lightestCount) {
        lightestCount = hostCount;
        lightestHost = host;
      }
    }
    return lightestHost;
  }
  
  private int getLightestPort(String host) {
    int lightestPort = 0;
    int lightestCount = Integer.MAX_VALUE;
    Map<Integer,List<RawSplit>> portMap = hostToPortToRawSplits.get(host);
    for (int port : portMap.keySet()) {
      int portCount = portMap.get(port).size();
      if (portCount < lightestCount) {
        lightestCount = portCount;
        lightestPort = port;
      }
    }
    return lightestPort;
  }
  
  private String getLightestHostAll() {
    String lightestHost = null;
    int lightestCount = Integer.MAX_VALUE;
    for (String host : hostToRawSplitCount.keySet()) {
      int hostCount = hostToRawSplitCount.get(host);
      if (hostCount < lightestCount) {
        lightestCount = hostCount;
        lightestHost = host;
      }
    }
    return lightestHost;
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
