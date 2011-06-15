package org.goldenorb;

import org.apache.zookeeper.ZooKeeper;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.event.OrbCallback;
import org.goldenorb.event.OrbExceptionEvent;

public class JobManager {
  
  private OrbCallback orbCallback;
  
  public JobManager(OrbCallback orbCallback){
    this.orbCallback = orbCallback;
  }
  
  private class JobStillActiveCheck implements Runnable, OrbConfigurable{
    
    private String jobID;
    private ZooKeeper zk;
    private OrbConfiguration orbConf;
    private boolean active = false;
    public JobStillActiveCheck(ZooKeeper zk, OrbConfiguration orbConf, String jobID){
      this.jobID = jobID;
    }
    
    @Override
    public void run() {
      synchronized(this){
        try {
          wait(orbConf.getJobHeartbeatTimeout());
        } catch (InterruptedException e) {
          orbCallback.process(new OrbExceptionEvent(e));
        }
        if(active != true){
          jobDeath(jobID);
        }
      }
    }

    private void jobDeath(String jobID2) {
      // TODO Auto-generated method stub
      
    }

    public void thump(){
      active = true;
    }
    
    @Override
    public void setOrbConf(OrbConfiguration orbConf) {
      this.orbConf = orbConf;
    }

    @Override
    public OrbConfiguration getOrbConf() {
      return orbConf;
    }
  }
}
