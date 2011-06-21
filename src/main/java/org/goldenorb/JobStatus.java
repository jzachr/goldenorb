package org.goldenorb;

public class JobStatus {
  public final static int ACTIVE = 1;
  public final static int COMPLETE = 2;
  public final static int DEAD = 3;
  
  private int status;
  
  public JobStatus(int status){
    this.status = status;
  }
  
  public boolean isActive(){
    return status == ACTIVE;
  }
  
  public boolean isComplete(){
    return status == COMPLETE;
  }
  
  public boolean isDead(){
    return status == DEAD;
  }
  
  public void setActive(){
    status = ACTIVE;
  }
  
  public void setComplete(){
    status = COMPLETE;
  }
  
  public void setDead(){
    status = DEAD;
  }
}
