package org.goldenorb;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.event.OrbCallback;
import org.goldenorb.event.OrbEvent;
import org.goldenorb.jet.OrbTrackerMember;
import org.goldenorb.jet.PartitionRequest;
import org.goldenorb.jet.PartitionRequestResponse;
import org.goldenorb.net.OrbDNS;
import org.goldenorb.types.message.BooleanMessage;
import org.goldenorb.types.message.RPCServer;
import org.goldenorb.zookeeper.LeaderGroup;
import org.goldenorb.zookeeper.Member;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrbTracker extends OrbTrackerMember implements Runnable, OrbConfigurable {
  
  private static final String ZK_BASE_PATH = "/GoldenOrb";
  
  private OrbConfiguration orbConf;
  
  private ZooKeeper zk;
  
  private LeaderGroup leaderGroup;
  
  private Logger LOG = LoggerFactory.getLogger(OrbTracker.class);
  
  private Server server = null;
  
  public static void main(String[] args) {
    OrbConfiguration orbConf = new OrbConfiguration(true);
    new Thread(new OrbTracker(orbConf)).start();
  }
  
  public OrbTracker(OrbConfiguration orbConf) {
    this.orbConf = orbConf;
  }
  
  public void run() {
    // get hostname
    try {
      setHostname(OrbDNS.getDefaultHost(orbConf));
      LOG.info("Starting OrbTracker on: " + getHostname());
    } catch (UnknownHostException e) {
      LOG.error("Unable to get hostname.", e);
      System.exit(-1);
    }
    
    // startServer
    try {
      server = RPC.getServer(this, getHostname(), getPort(), orbConf);
      server.start();
      
    } catch (IOException e) {
      LOG.error("Unable to get hostname.", e);
      System.exit(-1);
    }
    
    // connect to zookeeper
    try {
      establishZookeeperConnection();
    } catch (Exception e) {
      LOG.error("Failed to connect to Zookeeper", e);
      System.exit(-1);
    }
    
    // establish the zookeeper tree and join the cluster
    try {
      establishZookeeperTree();
    } catch (OrbZKFailure e) {
      LOG.error("Major Zookeeper Error: ", e);
      System.exit(-1);
    }
    
    if (leaderGroup.isLeader()) {
      executeAsLeader();
    } else {
      executeAsSlave();
    }
  }
  
  private void executeAsSlave() {
    // TODO Auto-generated method stub
    
  }
  
  private void executeAsLeader() {
    // TODO Auto-generated method stub
    
  }
  
  private void establishZookeeperTree() throws OrbZKFailure {
    ZookeeperUtils.notExistCreateNode(zk, ZK_BASE_PATH);
    ZookeeperUtils.notExistCreateNode(zk, ZK_BASE_PATH + "/" + orbConf.getOrbClusterName());
    ZookeeperUtils.notExistCreateNode(zk, ZK_BASE_PATH + "/" + orbConf.getOrbClusterName() + "/OrbTrackers");
    
    if (ZookeeperUtils.nodeExists(zk, ZK_BASE_PATH + "/" + orbConf.getOrbClusterName() + "/OrbTrackers/"
                                      + getHostname())) {
      LOG.info("Already have an OrbTracker on " + getHostname() + "(Exiting)");
      System.exit(-1);
    } else {
      ZookeeperUtils.tryToCreateNode(zk, ZK_BASE_PATH + "/" + orbConf.getOrbClusterName() + "/OrbTrackers/"
                                         + getHostname(), CreateMode.EPHEMERAL);
    }
    this.setAvailablePartitions(orbConf.getNumberOfPartitionsPerMachine());
    this.setInUsePartitions(0);
    this.setReservedPartitions(0);
    this.setLeader(false);
    this.setPartitionCapacity(orbConf.getNumberOfPartitionsPerMachine());
    leaderGroup = new LeaderGroup<OrbTrackerMember>(zk, new OrbTrackerCallback(),
        ZK_BASE_PATH + "/" + orbConf.getOrbClusterName() + "/OrbTrackerLeaderGroup", this,
        OrbTrackerMember.class);
  }
  
  public class OrbTrackerCallback implements OrbCallback {
    
    public void process(OrbEvent e) {

    }
    
  }
  
  private void establishZookeeperConnection() throws IOException, InterruptedException {
    zk = ZookeeperUtils.connect(orbConf.getOrbZooKeeperQuorum());
  }
  
  public void setOrbConf(OrbConfiguration orbConf) {
    this.orbConf = orbConf;
  }
  
  public OrbConfiguration getOrbConf() {
    return orbConf;
  }
  
  private void collectAggregateStats() {
    // TODO Should get collect all of the OrbMembers from 
  }
  
  private void launchProcess(){
    // TODO Current stub this should get passed the OrbConf and should 
  }
}
