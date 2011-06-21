package org.goldenorb;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrbRunner {
  
  private static Logger logger;
  protected static ZooKeeper ZK;
  
  public OrbRunner() {
    logger = LoggerFactory.getLogger(OrbRunner.class);
  }
  
  public String runJob(OrbConfiguration orbConf) {
    String jobNumber = null;
    try {
      try {
        ZK = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
      } catch (IOException e) {
        logger.info("Failed to connect to zookeeper on " + orbConf.getOrbZooKeeperQuorum());
        logger.error("IOException", e);
      } catch (InterruptedException e) {
        logger.info("Failed to connect to zookeeper on " + orbConf.getOrbZooKeeperQuorum());
        logger.error("InterruptedException", e);
      }
      
      // create JobQueue path "/GoldenOrb/<cluster name>/JobQueue" if it doesn't already exist
      ZookeeperUtils.notExistCreateNode(ZK, "/GoldenOrb", CreateMode.PERSISTENT);
      ZookeeperUtils.notExistCreateNode(ZK, "/GoldenOrb/" + orbConf.getOrbClusterName(),
        CreateMode.PERSISTENT);
      ZookeeperUtils.notExistCreateNode(ZK, "/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobQueue",
        CreateMode.PERSISTENT);
      
      // create the sequential Job using orbConf
      jobNumber = ZookeeperUtils.notExistCreateNode(ZK, "/GoldenOrb/" + orbConf.getOrbClusterName() + "/JobQueue/Job",
        orbConf, CreateMode.PERSISTENT_SEQUENTIAL);
      
    } catch (Exception e) {
      logger.info("Cluster does not exist in ZooKeeper on " + orbConf.getOrbZooKeeperQuorum());
      logger.error("Exception", e);
    }
    return jobNumber;
  }
}
