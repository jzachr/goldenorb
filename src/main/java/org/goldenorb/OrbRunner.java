package org.goldenorb;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a basis upon which to run a Job given a specified OrbConfiguration. It should be
 * extended to actually run a Job within GoldenOrb.
 * 
 * @author long
 * 
 */
public class OrbRunner {
  
  private static Logger logger;
  protected static ZooKeeper ZK;
  
  /**
   * Constructs an OrbRunner object.
   */
  public OrbRunner() {
    logger = LoggerFactory.getLogger(OrbRunner.class);
  }
  
  /**
   * This is the runJob method that begins a Job. It first attempts to connect to the ZooKeeper server as
   * defined in the OrbConfiguration, then tries to create the /GoldenOrb, /GoldenOrb/ClusterName, and
   * /GoldenOrb/ClusterName/JobQueue nodes if they do not already exist. Then, it creates the Job itself as a
   * PERSISTENT_SEQUENTIAL node in the form of JobXXXXXXXXXX.
   * 
   * @param orbConf
   *          - The OrbConfiguration for a specific Job.
   * @exception - IOException
   * @exception - InterruptedException
   * @return jobNumber
   */
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
      jobNumber = ZookeeperUtils.notExistCreateNode(ZK, "/GoldenOrb/" + orbConf.getOrbClusterName()
                                                        + "/JobQueue/Job", orbConf,
        CreateMode.PERSISTENT_SEQUENTIAL);
      
    } catch (Exception e) {
      logger.info("Cluster does not exist in ZooKeeper on " + orbConf.getOrbZooKeeperQuorum());
      logger.error("Exception", e);
    }
    return jobNumber;
  }
}
