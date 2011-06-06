package org.goldenorb.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ConnectWatcher implements Watcher {
	
	private static final int SESSION_TIMEOUT = 10000;
	private CountDownLatch connectedSignal = new CountDownLatch(1);
		
	public ZooKeeper connect(String hosts) throws IOException, InterruptedException {
		ZooKeeper _zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		connectedSignal.await();
		return _zk;
	}

	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			connectedSignal.countDown();
		}		
	}
}
