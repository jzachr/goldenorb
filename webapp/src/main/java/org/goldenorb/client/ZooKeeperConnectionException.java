package org.goldenorb.client;



import com.google.gwt.user.client.rpc.SerializableException;

public class ZooKeeperConnectionException extends SerializableException {
  
  private String errorMessage;
  
  public ZooKeeperConnectionException() { 
    errorMessage = "ZooKeeperConnectionException";
  }
  
  public ZooKeeperConnectionException(Exception e) {
    StringBuilder error = new StringBuilder();
    error.append("ZooKeeperConnectionException\n");
    error.append("Caused By :\n");
    error.append("\t"+e.toString()+"\n");
    StackTraceElement[] stackTrace = e.getStackTrace();
    for(StackTraceElement ste : stackTrace) {
      error.append("\t\t" + ste.toString() + "\n");
    }
    errorMessage = error.toString();
  }
  
  public String getErrorMessage() {
    return errorMessage;
  }
  
  public void setErrorMessage(String message) {
    errorMessage = message;
  }
  
}
