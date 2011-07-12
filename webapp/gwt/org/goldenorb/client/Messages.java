package org.goldenorb.client;

/**
 * Interface to represent the messages contained in resource bundle:
 * 	/home/rebanks/workspaces/buildtests/org.goldenorb.gui/src/main/resources/org/goldenorb/client/Messages.properties'.
 */
public interface Messages extends com.google.gwt.i18n.client.Messages {
  
  /**
   * Translated "Enter your name".
   * 
   * @return translated "Enter your name"
   */
  @DefaultMessage("Enter your name")
  @Key("nameField")
  String nameField();

  /**
   * Translated "Send".
   * 
   * @return translated "Send"
   */
  @DefaultMessage("Send")
  @Key("sendButton")
  String sendButton();
}
