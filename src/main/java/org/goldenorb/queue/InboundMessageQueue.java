package org.goldenorb.queue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.goldenorb.Message;
import org.goldenorb.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InboundMessageQueue {
	Map<String, List<Message<? extends Writable>>> inboundMessageMap = new HashMap<String, List<Message<? extends Writable>>>();
	Set<String> verticesWithMessages = new HashSet<String>();
	private Logger logger;
	
	public Map<String, List<Message<? extends Writable>>> getInboundMessageMap() {
		return inboundMessageMap;
	}

	public Set<String> getVerticesWithMessages() {
		return verticesWithMessages;
	}

	public InboundMessageQueue(Collection<String> ids){
		logger = LoggerFactory.getLogger(InboundMessageQueue.class);
		for(String id: ids){
			inboundMessageMap.put(id, Collections.synchronizedList(new ArrayList<Message<? extends Writable>>()));
		}
	}
	
	public void addMessages(Messages ms){
		for(Message<? extends Writable> m: ms.getList()){
			addMessage(m);
		}
	}
	
	public void addMessage(Message<? extends Writable> m){	
		inboundMessageMap.get(m.getDestinationVertex()).add(m);
		synchronized(verticesWithMessages){
			verticesWithMessages.add(m.getDestinationVertex());
		
		}
	}
	
	public void addNewVertex(String id){
		synchronized(inboundMessageMap){
			inboundMessageMap.put(id, Collections.synchronizedList(new ArrayList<Message<? extends Writable>>()));
		}
	}
	
	public List<Message<? extends Writable>> getMessage(String vertexID){
		return inboundMessageMap.get(vertexID);
	}
}
