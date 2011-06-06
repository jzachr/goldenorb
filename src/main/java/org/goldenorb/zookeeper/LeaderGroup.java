package org.goldenorb.zookeeper;

import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.event.OrbCallback;

public class LeaderGroup<MEMBER_TYPE extends Member> {
	
	private boolean leader;
	private OrbCallback orbCallback;
	private String basePath;
	private MEMBER_TYPE member;
	private ZooKeeper zk;
	private MEMBER_TYPE memberLeader;
	private SortedMap<String, MEMBER_TYPE> members = new TreeMap<String, MEMBER_TYPE>();
	
	public LeaderGroup(ZooKeeper zk, OrbCallback orbCallback, String path, MEMBER_TYPE member) {
		this.orbCallback = orbCallback;
		this.basePath = path;
		this.member = member;
		this.zk = zk;
		try {
			init();
		} catch (OrbZKFailure e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void init() throws OrbZKFailure{
		ZookeeperUtils.notExistCreateNode(zk, basePath);
		String path = ZookeeperUtils.tryToCreateNode(zk, basePath + "/member", member,CreateMode.EPHEMERAL_SEQUENTIAL);
		members.put(path, member);
	}
	
	public void updateMembers(){
	}
}
