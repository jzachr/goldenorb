package org.goldenorb.client;

import com.google.gwt.user.client.rpc.SerializableException;

/**
 * Serializable Exception to notify users nodes that are suppose to be monitored do not currently exist.
 */
@SuppressWarnings("deprecation")
public class NodeDoesNotExistException extends SerializableException {
  
  private String errorMessage = "NodeDoesNotExistException :\n\tNODE DOES NOT EXIST : ";
  
  public NodeDoesNotExistException() {}
  
  public NodeDoesNotExistException(String path) {
    errorMessage = errorMessage + path;
  }
  
  public void setErrorMessage(String message) {
    errorMessage = message;
  }
  
  public String getErrorMessage() {
    return errorMessage;
  }
}
