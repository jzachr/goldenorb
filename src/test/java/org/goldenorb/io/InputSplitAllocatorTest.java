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

import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.io.input.RawSplit;
import org.goldenorb.jet.OrbPartitionMember;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class InputSplitAllocatorTest {
  
  static MiniDFSCluster cluster;
  static FileSystem fs;
  
  @BeforeClass
  public static void setUpCluster() throws Exception {
    Configuration conf = new Configuration(true);
    cluster = new MiniDFSCluster(conf, 1, true, null);
    fs = cluster.getFileSystem();
    
  }
  
  @Test
  public void testInputSplitAllocator() throws Exception {
    
    OrbConfiguration orbConf = new OrbConfiguration(true);
    orbConf.set("fs.default.name", "hdfs://localhost:" + cluster.getNameNodePort());
    orbConf.setJobNumber("0");
    
    List<OrbPartitionMember> orbPartitionMembersList = new ArrayList<OrbPartitionMember>();
    
    OrbPartitionMember opMember1 = new OrbPartitionMember();
    
    opMember1.setOrbConf(orbConf);
    opMember1.setPartitionID(0);
    opMember1.setHostname("localhost");
    opMember1.setPort(3100);
    
    OrbPartitionMember opMember2 = new OrbPartitionMember();
    
    opMember2.setOrbConf(orbConf);
    opMember2.setPartitionID(1);
    opMember2.setHostname("localhost");
    opMember2.setPort(3101);
    
    OrbPartitionMember opMember3 = new OrbPartitionMember();
    
    opMember3.setOrbConf(orbConf);
    opMember3.setPartitionID(2);
    opMember3.setHostname("localhost");
    opMember3.setPort(3102);
    
    orbPartitionMembersList.add(opMember1);
    orbPartitionMembersList.add(opMember2);
    orbPartitionMembersList.add(opMember3);
    
    InputSplitAllocator isa = new InputSplitAllocator(orbConf, orbPartitionMembersList);
    
    RawSplit rs1 = new RawSplit();
    RawSplit rs2 = new RawSplit();
    RawSplit rs3 = new RawSplit();
    
    Text txt = new Text("test");
    rs1.setBytes(ZookeeperUtils.writableToByteArray(txt), 0, ZookeeperUtils.writableToByteArray(txt).length);
    rs2.setBytes(ZookeeperUtils.writableToByteArray(txt), 0, ZookeeperUtils.writableToByteArray(txt).length);
    rs3.setBytes(ZookeeperUtils.writableToByteArray(txt), 0, ZookeeperUtils.writableToByteArray(txt).length);
    
    String[] locations = {"localhost"};
    
    rs1.setLocations(locations);
    rs2.setLocations(locations);
    rs3.setLocations(locations);

    ArrayList<RawSplit> rsList = new ArrayList<RawSplit>();
    rsList.add(rs1);
    rsList.add(rs2);
    rsList.add(rs3);
    
    isa.assignInputSplits(rsList);
    
    
    
    /*
    Text txt2 = new Text();
    Text txt3 = new Text();
    txt3 = (Text) ZookeeperUtils.byteArrayToWritable(rs.getBytes().getBytes(), txt2);
    System.out.println(txt3);
    */
    
    assertNotNull(InputSplitAllocator.class);
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