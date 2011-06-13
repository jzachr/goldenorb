package org.goldenorb.event;

public class OrbExceptionEvent extends OrbEvent {
  
  private Exception exception;
  
  public OrbExceptionEvent(Exception exception){
    super(OrbEvent.ORB_EXCEPTION);
    this.setException(exception);
  }

  public void setException(Exception exception) {
    this.exception = exception;
  }

  public Exception getException() {
    return exception;
  }
}
