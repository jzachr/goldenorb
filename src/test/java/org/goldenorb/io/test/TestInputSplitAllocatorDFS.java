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
package org.goldenorb.io.test;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.io.InputSplitAllocator;
import org.goldenorb.io.input.RawSplit;
import org.goldenorb.jet.OrbPartitionMember;
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
    cluster = new MiniDFSCluster(conf, 1, true, null);
    fs = cluster.getFileSystem();
    System.out.println(FileSystem.getDefaultUri(conf));
  }
  
  @Test
  public void testInputSplitAllocator() throws Exception {
    LOG = LoggerFactory.getLogger(TestInputSplitAllocatorDFS.class);
    
    Random rand = new Random();
    
    List<DataNode> dnList = cluster.getDataNodes();
    for (DataNode dn : dnList) {
      System.out.println("DataNode hostname: " + dn.getSelfAddr().getHostName());
      System.out.println("DataNode port: " + dn.getSelfAddr().getPort());
    }
    
    System.out.println("NameNode hostname: " + cluster.getNameNode().getNameNodeAddress().getHostName());
    System.out.println("NameNode port: " + cluster.getNameNode().getNameNodeAddress().getPort());
    
    //DFSTestUtil.createFile(fs, new Path("test/inpath"), 400000000L, (short) 2, rand.nextLong());
    fs.copyFromLocalFile(new Path("src/test/resources/InputSplitAllocatorDFSTestData.txt"), new Path("test/inpath"));
    
    OrbConfiguration orbConf = new OrbConfiguration();
    orbConf.set("fs.default.name", "hdfs://localhost:" + cluster.getNameNodePort());
    orbConf.setJobNumber("0");
    orbConf.setFileInputPath("test/inpath");
    orbConf.setNameNode("hdfs://localhost:" + cluster.getNameNodePort());
    orbConf.setFileInputFormatClass(TextInputFormat.class);
    orbConf.setFileOutputFormatClass(TextInputFormat.class);
    
    OrbPartitionMember opm1 = new OrbPartitionMember();
    opm1.setHostname(dnList.get(0).getSelfAddr().getHostName());
    opm1.setPort(dnList.get(0).getSelfAddr().getPort());
    opm1.setOrbConf(orbConf);
    OrbPartitionMember opm2 = new OrbPartitionMember();
    opm2.setHostname(dnList.get(0).getSelfAddr().getHostName());
    opm2.setPort(1);
    opm2.setOrbConf(orbConf);
    OrbPartitionMember opm3 = new OrbPartitionMember();
    opm3.setHostname(dnList.get(0).getSelfAddr().getHostName());
    opm3.setPort(2);
    opm3.setOrbConf(orbConf);
    OrbPartitionMember opm4 = new OrbPartitionMember();
    opm4.setHostname(dnList.get(0).getSelfAddr().getHostName());
    opm4.setPort(3);
    opm4.setOrbConf(orbConf);
    OrbPartitionMember opm5 = new OrbPartitionMember();
    opm5.setHostname(dnList.get(0).getSelfAddr().getHostName());
    opm5.setPort(4);
    opm5.setOrbConf(orbConf);
    OrbPartitionMember opm6 = new OrbPartitionMember();
    opm6.setHostname(dnList.get(0).getSelfAddr().getHostName());
    opm6.setPort(5);
    opm6.setOrbConf(orbConf);
    
    List<OrbPartitionMember> orbPartitionMembers = new ArrayList<OrbPartitionMember>();
    orbPartitionMembers.add(opm1);
    orbPartitionMembers.add(opm2);
    orbPartitionMembers.add(opm3);
    orbPartitionMembers.add(opm4);
    orbPartitionMembers.add(opm5);
    orbPartitionMembers.add(opm6);
    
    // LOG.info(fs.getContentSummary(new Path("test/inpath")).toString());
    
    InputSplitAllocator isa = new InputSplitAllocator(orbConf, orbPartitionMembers);
    Map<OrbPartitionMember,List<RawSplit>> inputSplitAssignments = isa.assignInputSplits();
    for (OrbPartitionMember orbPartitionMember : inputSplitAssignments.keySet()) {
      for (RawSplit rsplit : inputSplitAssignments.get(orbPartitionMember)) {
        LOG.info(orbPartitionMember.getHostname() + ":" + orbPartitionMember.getPort() + " | "
                 + inputSplitAssignments.get(orbPartitionMember));
        String[] s = rsplit.getLocations();
        for (String loc : s) {
          System.out.println("RawSplit locations: " + loc);
        }
        System.out.println(rsplit.getDataLength());
      }
      assertTrue(inputSplitAssignments.get(orbPartitionMember).size() < 2);
    }
    
  }
  
  @AfterClass
  public static void tearDownCluster() throws IOException {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
