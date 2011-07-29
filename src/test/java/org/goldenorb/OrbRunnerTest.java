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
package org.goldenorb;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.zookeeper.KeeperException;
import org.goldenorb.OrbRunner;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;

/**
 * Tests OrbRunner by first running a Job with the default OrbConfiguration. Then, the test looks for Jobs
 * under the JobQueue and looks to see if the OrbConfiguration cluster name property is the same coming out as it is going in.
 * 
 */
public class OrbRunnerTest extends OrbRunner {
  
  private static OrbConfiguration orbConf = new OrbConfiguration(true); // default configuration, also assuming ZooKeeper is
                                                         // running on localhost:21810
  
  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  
  @BeforeClass
  public static void setUpCluster() throws Exception {
    orbConf.setOrbClusterName("TestOrbCluster");
    orbConf.setOrbZooKeeperQuorum("localhost:21810");
    cluster = new MiniDFSCluster(orbConf, 3, true, null);
    orbConf.set("fs.default.name", "hdfs://localhost:" + cluster.getNameNodePort());
  }
  
  @Test
  public void testOrbRunner() throws Exception {
    
    runJob(orbConf);
    
    List<String> jobList = ZK.getChildren("/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobQueue", false);
    for (String jobName : jobList) {
      OrbConfiguration compareOrbConf = (OrbConfiguration) ZookeeperUtils
          .getNodeWritable(ZK, "/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobQueue/" + jobName,
            OrbConfiguration.class, orbConf);
      assertEquals(compareOrbConf.getOrbClusterName(), orbConf.getOrbClusterName());
    }
  }
  
  @Test
  public void testDistributeFiles() throws IOException, KeeperException, InterruptedException, OrbZKFailure {
    OrbConfiguration orbConf = new OrbConfiguration(true);
    orbConf.setOrbClusterName("TestOrbCluster");
    orbConf.setOrbZooKeeperQuorum("localhost:21810");
    orbConf.set("fs.default.name", "hdfs://localhost:" + cluster.getNameNodePort());
    orbConf.addFileToDistribute("src/test/resources/distributeTest1.txt");
    orbConf.addFileToDistribute("src/test/resources/distributeTest2.txt");
    orbConf.addFileToDistribute("src/test/resources/HelloWorld.jar");
    runJob(orbConf);
    FileSystem fs = cluster.getFileSystem();
    //Files were copied from local to HDFS
    assertTrue(fs.exists(new Path("/DistributeFiles/distributeTest1.txt")));
    assertTrue(fs.exists(new Path("/DistributeFiles/distributeTest2.txt")));
    assertTrue(fs.exists(new Path("/DistributeFiles/HelloWorld.jar")));
    // Check Paths are set in orbConfiguration
    Path[] localFiles = orbConf.getHDFSdistributedFiles();
    for (Path path : localFiles) {
      System.out.println(path.toString());
    }
    assertEquals(3, localFiles.length);
    
    List<String> jobList = ZK.getChildren("/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobQueue", false);
    for (String jobName : jobList) {
      OrbConfiguration compareOrbConf = (OrbConfiguration) ZookeeperUtils
          .getNodeWritable(ZK, "/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobQueue/" + jobName,
            OrbConfiguration.class, orbConf);
      assertEquals(compareOrbConf.getHDFSdistributedFiles(), orbConf.getHDFSdistributedFiles());
    }
  }
  
/**
 * 
 */
  @After
  public void cleanUpOrbRunner() throws Exception {
    ZookeeperUtils.recursiveDelete(ZK, "/GoldenOrb");
    ZookeeperUtils.deleteNodeIfEmpty(ZK, "/GoldenOrb");
    
    ZK.close();
  }
}
