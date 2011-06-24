package org.goldenorb.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.zookeeper.OrbZKFailure;
import org.goldenorb.zookeeper.ZookeeperUtils;

public class EmptyJobQueues {
  public static void main(String[] args) throws IOException, InterruptedException, OrbZKFailure {
    ZooKeeper zk = ZookeeperUtils.connect("localhost");
    ZookeeperUtils.recursiveDelete(zk, "/GoldenOrb/OrbCluster");
  }
}
