package org.goldenorb.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.*;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class OrbSimpleZKTest {
  @Test
  public void simpleTest() {
    assertTrue(true);
  }
  
  @Test
  public void connectToZooKeeper() throws IOException, InterruptedException{
    ZooKeeper zk = new ZooKeeper("localhost", 5000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        System.out.println(event);
      }
    });
    
    zk.close();
  }
}
