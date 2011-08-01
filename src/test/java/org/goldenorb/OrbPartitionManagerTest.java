package org.goldenorb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.jet.PartitionRequest;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrbPartitionManagerTest {
  
  private final Logger logger = LoggerFactory.getLogger(OrbPartitionManagerTest.class);
  private static ZooKeeper ZK;
  private static OrbConfiguration orbConf;

  @BeforeClass
  public static void setUpTest() throws IOException, InterruptedException, OrbZKFailure {
    orbConf = new OrbConfiguration(true);
    orbConf.setOrbZooKeeperQuorum("localhost:21810");
    ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
  }
  
  @Test
  public void testOrbPartitionManager() {
    int requestedPartitions = 3;
    int reservedPartitions = 2;
    int totalPartitions = requestedPartitions + reservedPartitions;
    orbConf.setJobNumber("0");
    OrbPartitionManager<OrbPartitionProcess> OPManager = new OrbPartitionManager<OrbPartitionProcess>(
        orbConf, OrbPartitionProcess.class);
    assertEquals(OPManager.getOrbConf(), orbConf);
    OPManager.setOrbConf(orbConf);
    assertEquals(OPManager.getPartitionProcessClass(), OrbPartitionProcess.class);
    OPManager.setPartitionProcessClass(OrbPartitionProcess.class);
    logger.info("OrbPartitionManager IP: " + OPManager.getIpAddress());
    PartitionRequest request = new PartitionRequest();
    request.setReservedPartitions(reservedPartitions);
    request.setActivePartitions(requestedPartitions);
    request.setJobID("0");
    request.setBasePartitionID(0);
    request.setJobConf(orbConf);
    try {
      OPManager.launchPartitions(request);
      Map<String, List<OrbPartitionProcess>> mapOPP = OPManager.getProcessesByJob();
      assertTrue(mapOPP.get("0").size() == totalPartitions);
      OPManager.kill("0");
    } catch (InstantiationException e) {
      logger.error("InstantiationException.");
    } catch (IllegalAccessException e) {
      logger.error("IllegalAccessException.");
    }
  }
}
