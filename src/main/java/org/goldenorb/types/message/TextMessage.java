package org.goldenorb.types.message;

import org.apache.hadoop.io.Text;
import org.goldenorb.Message;

public class TextMessage extends Message<Text> {
	public TextMessage() {
		super(Text.class);
	}
	public TextMessage(String destinationVertex, Text messageValue){
		super(Text.class);
		this.setDestinationVertex(destinationVertex);
		this.setMessageValue(messageValue);
	}
	public TextMessage(String destinationVertex, String value){
		super(Text.class);
		this.setDestinationVertex(destinationVertex);
		this.setMessageValue(new Text(value));
	}
	public String get(){
		return ((Text)this.getMessageValue()).toString();
	}
	public void set(String value){
		((Text)this.getMessageValue()).set(value);
	}
}
