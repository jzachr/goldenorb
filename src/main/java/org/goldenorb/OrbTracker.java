package org.goldenorb;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.event.OrbCallback;
import org.goldenorb.event.OrbEvent;
import org.goldenorb.net.OrbDNS;
import org.goldenorb.zookeeper.LeaderGroup;
import org.goldenorb.zookeeper.Member;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrbTracker implements Runnable, OrbConfigurable {
	
	private static final String ZK_BASE_PATH = "/GoldenOrb";
	
	private OrbConfiguration orbConf;
	
	private String hostname;
	private ZooKeeper zk;
	
	private boolean leader;
	private LeaderGroup leaderGroup;
	
	
	private Logger LOG = LoggerFactory.getLogger(OrbTracker.class);
	
	public static void main(String[] args){
		OrbConfiguration orbConf = new OrbConfiguration(true);
		new Thread(new OrbTracker(orbConf)).start();
	}
	
	public OrbTracker(OrbConfiguration orbConf){
		this.orbConf = orbConf;
	}
	
	public void run() {
		//get hostname
		try {
			hostname = OrbDNS.getDefaultHost(orbConf);
			LOG.info("Starting OrbTracker on: " + hostname);
		} catch (UnknownHostException e) {
			LOG.error("Unable to get hostname.", e);
			System.exit(-1);
		}
		
		//connect to zookeeper
		try {
			establishZookeeperConnection();
		} catch (Exception e) {
			LOG.error("Failed to connect to Zookeeper", e);
			System.exit(-1);
		}
		
		//establish the zookeeper tree and join the cluster
		try {
			establishZookeeperTree();
		} catch (OrbZKFailure e) {
			LOG.error("Major Zookeeper Error: ", e);
			System.exit(-1);
		}
	}

	private void establishZookeeperTree() throws OrbZKFailure {
		ZookeeperUtils.notExistCreateNode(zk, ZK_BASE_PATH);
		ZookeeperUtils.notExistCreateNode(zk, ZK_BASE_PATH + "/" + orbConf.getOrbClusterName());
		ZookeeperUtils.notExistCreateNode(zk, ZK_BASE_PATH + "/" + orbConf.getOrbClusterName() + "/OrbTrackers");
		
		if(ZookeeperUtils.nodeExists(zk, ZK_BASE_PATH + "/" + orbConf.getOrbClusterName() + "/OrbTrackers/" + hostname)){
			LOG.info("Already have an OrbTracker on " + hostname + "(Exiting)");
			System.exit(-1);
		} else{
			ZookeeperUtils.tryToCreateNode(zk, ZK_BASE_PATH + "/" + orbConf.getOrbClusterName() + "/OrbTrackers/" + hostname, CreateMode.EPHEMERAL);
		}
		
		leaderGroup = new LeaderGroup<Member>(zk, new OrbTrackerCallback(), ZK_BASE_PATH + "/" + orbConf.getOrbClusterName() + "/OrbTrackerLeaderGroup", new OrbTrackerMember());
	}

	public class OrbTrackerCallback implements OrbCallback{

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

}
