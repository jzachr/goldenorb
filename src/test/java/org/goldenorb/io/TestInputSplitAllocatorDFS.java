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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.io.InputSplitAllocator;
import org.goldenorb.io.input.RawSplit;
import org.goldenorb.jet.OrbPartitionMember;
import org.goldenorb.net.OrbDNS;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestInputSplitAllocatorDFS {
  
  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  
  private Logger LOG;
  
  @BeforeClass
  public static void setUpCluster() throws Exception {
    Configuration conf = new Configuration(true);
    conf.set("dfs.block.size", "16384");
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = cluster.getFileSystem();
  }
  
  @Test
  public void testInputSplitAllocator() throws Exception {
    LOG = LoggerFactory.getLogger(TestInputSplitAllocatorDFS.class);
    
    fs.copyFromLocalFile(new Path("src/test/resources/InputSplitAllocatorDFSTestData.txt"), new Path(
        "test/inpath"));
    
    OrbConfiguration orbConf = new OrbConfiguration(true);
    orbConf.set("fs.default.name", "hdfs://localhost:" + cluster.getNameNodePort());
    orbConf.setJobNumber("0");
    orbConf.setFileInputPath("test/inpath");
    orbConf.setNameNode("hdfs://localhost:" + cluster.getNameNodePort());
    
    String hostname = OrbDNS.getDefaultHost(orbConf);
    if (hostname.endsWith(".")) {
      hostname = hostname.substring(0, hostname.length() - 1);
    }
    
    OrbPartitionMember opm1 = new OrbPartitionMember();
    opm1.setHostname(hostname);
    opm1.setPort(0);
    OrbPartitionMember opm2 = new OrbPartitionMember();
    opm2.setHostname(hostname);
    opm2.setPort(1);
    OrbPartitionMember opm3 = new OrbPartitionMember();
    opm3.setHostname(hostname);
    opm3.setPort(2);
    OrbPartitionMember opm4 = new OrbPartitionMember();
    opm4.setHostname(hostname);
    opm4.setPort(3);
    OrbPartitionMember opm5 = new OrbPartitionMember();
    opm5.setHostname(hostname);
    opm5.setPort(4);
    OrbPartitionMember opm6 = new OrbPartitionMember();
    opm6.setHostname(hostname);
    opm6.setPort(5);
    
    List<OrbPartitionMember> orbPartitionMembers = new ArrayList<OrbPartitionMember>();
    orbPartitionMembers.add(opm1);
    orbPartitionMembers.add(opm2);
    orbPartitionMembers.add(opm3);
    orbPartitionMembers.add(opm4);
    orbPartitionMembers.add(opm5);
    orbPartitionMembers.add(opm6);
    
    InputSplitAllocator isa = new InputSplitAllocator(orbConf, orbPartitionMembers);
    Map<OrbPartitionMember,List<RawSplit>> inputSplitAssignments = isa.assignInputSplits();
    long totalFileSize = 0;
    for (OrbPartitionMember orbPartitionMember : inputSplitAssignments.keySet()) {
      long rawSplitSize = 0;
      for (RawSplit rSplit : inputSplitAssignments.get(orbPartitionMember)) {
        rawSplitSize += rSplit.getDataLength();
      }
      totalFileSize += rawSplitSize;
      
      LOG.info(orbPartitionMember.getHostname() + ":" + orbPartitionMember.getPort() + " | RawSplits count: "
               + inputSplitAssignments.get(orbPartitionMember).size() + " | RawSplits size: " + rawSplitSize);
      
      assertTrue(inputSplitAssignments.get(orbPartitionMember).size() <= 5);
    }
    
    File testFile = new File("src/test/resources/InputSplitAllocatorDFSTestData.txt");
    assertTrue(totalFileSize == testFile.length());
  }
  
  @AfterClass
  public static void tearDownCluster() throws IOException {
    if (fs != null) {
      fs.close();
    }
  }
}
