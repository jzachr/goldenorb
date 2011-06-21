package org.goldenorb.event;

public class OrbEvent {
  public final static int LOST_MEMBER = 1;
  public final static int NEW_MEMBER = 2;
  public final static int LEADERSHIP_CHANGE = 3;
  public final static int NEW_JOB = 4;
  public final static int JOB_COMPLETE = 5;
  public final static int JOB_DEATH = 6;
  public final static int ORB_EXCEPTION = 8;
  public final static int MEMBER_DATA_CHANGE = 9;
  public final static int JOB_START = 10;

  int type;

  public OrbEvent(int type){
    this.type = type;
  }
   
  public int getType(){
    return type;
  }
  public void setType(int type){
    this.type = type;
  }
}
