package org.goldenorb.zookeeper;

import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;


public abstract class OrbBarrier implements OrbConfigurable{
	
	public OrbBarrier(OrbConfiguration orbConf, String barrierName, int numOfMembers, String member, ZooKeeper zk) {
	}
	
	public abstract void enter() throws OrbZKFailure;
}
