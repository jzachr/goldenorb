package org.goldenorb.event;

public class OrbExceptionEvent implements OrbEvent {
  
  private Exception exception;
  
  public OrbExceptionEvent(Exception exception){
    this.setException(exception);
  }

  public void setException(Exception exception) {
    this.exception = exception;
  }

  public Exception getException() {
    return exception;
  }
}
