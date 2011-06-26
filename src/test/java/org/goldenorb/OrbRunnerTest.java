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

import org.goldenorb.OrbRunner;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;

import java.util.List;

import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.zookeeper.ZookeeperUtils;

/**
 * Tests OrbRunner by first running a Job with the default OrbConfiguration. Then, the test looks for Jobs
 * under the JobQueue and looks to see if the OrbConfiguration cluster name property is the same coming out as it is going in.
 * 
 * @author long
 * 
 */
public class OrbRunnerTest extends OrbRunner {
  
  OrbConfiguration orbConf = new OrbConfiguration(true); // default configuration, also assuming ZooKeeper is
                                                         // running on localhost:2181
  
  @Test
  public void testOrbRunner() throws Exception {
    orbConf.setOrbClusterName("TestOrbCluster");
    runJob(orbConf);
    
    List<String> jobList = ZK.getChildren("/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobQueue", false);
    for (String jobName : jobList) {
      OrbConfiguration compareOrbConf = (OrbConfiguration) ZookeeperUtils
          .getNodeWritable(ZK, "/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobQueue/" + jobName,
            OrbConfiguration.class, orbConf);
      assertEquals(compareOrbConf.getOrbClusterName(), orbConf.getOrbClusterName());
    }
  }
  
/**
 * 
 */
  @After
  public void cleanUpOrbRunner() throws Exception {
    List<String> jobList = ZK.getChildren("/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobQueue", false);
    
    // delete all Jobs
    for (String jobName : jobList) {
      ZookeeperUtils.deleteNodeIfEmpty(ZK, "/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobQueue/"
                                           + jobName);
    }
    
    // delete entire JobQueue path
    ZookeeperUtils.deleteNodeIfEmpty(ZK, "/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobQueue");
    ZookeeperUtils.deleteNodeIfEmpty(ZK, "/GoldenOrb/" + orbConf.getOrbClusterName());
    ZookeeperUtils.deleteNodeIfEmpty(ZK, "/GoldenOrb");
    
    ZK.close();
  }
}
