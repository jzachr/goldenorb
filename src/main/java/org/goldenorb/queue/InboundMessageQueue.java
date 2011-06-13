package org.goldenorb.queue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.goldenorb.Message;
import org.goldenorb.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InboundMessageQueue {
	Map<String, List<Message>> inboundMessageMap = new HashMap<String, List<Message>>();
	Set<String> verticesWithMessages = new HashSet<String>();
	private Logger logger;
	
	public Map<String, List<Message>> getInboundMessageMap() {
		return inboundMessageMap;
	}

	public Set<String> getVerticesWithMessages() {
		return verticesWithMessages;
	}

	public InboundMessageQueue(Collection<String> ids){
		logger = LoggerFactory.getLogger(InboundMessageQueue.class);
		for(String id: ids){
			inboundMessageMap.put(id, Collections.synchronizedList(new ArrayList<Message>()));
			//logger.info(id);
		}
	}
	
	public void addMessages(Messages ms){
		for(Message m: ms.getList()){
			addMessage(m);
		}
	}
	
	public void addMessage(Message m){
		//logger.info(m.getDestinationVertex());
		if(m == null){
			System.err.println("It is null");
		}
				
		inboundMessageMap.get(m.getDestinationVertex()).add(m);
		synchronized(verticesWithMessages){
			verticesWithMessages.add(m.getDestinationVertex());
		
		}
	}
	
	public void addNewVertex(String id){
		synchronized(inboundMessageMap){
			inboundMessageMap.put(id, Collections.synchronizedList(new ArrayList<Message>()));
		}
	}
	
	public List<Message> getMessage(String vertexID){
		return inboundMessageMap.get(vertexID);
	}
}
