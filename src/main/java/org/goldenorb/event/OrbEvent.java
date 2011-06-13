package org.goldenorb.event;

public class OrbEvent {
  final static int LOST_MEMBER = 1;
  final static int NEW_MEMBER = 2;
  final static int LEADERSHIP_CHANGE = 3;
  final static int NEW_JOB = 4;
  final static int JOB_COMPLETE = 5;
  final static int JOB_DEATH = 6;
  final static int JOB_HEARTBEAT = 7;
  final static int ORB_EXCEPTION = 8;

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
