package org.goldenorb;

import org.goldenorb.OrbRunner;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;

import java.util.List;

import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.zookeeper.ZookeeperUtils;

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
    
    System.out.println(ZK.getChildren("/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobQueue", false));
    
    assertThat(OrbRunner.class, notNullValue());
    assertThat(orbConf, notNullValue());
  }
  
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
