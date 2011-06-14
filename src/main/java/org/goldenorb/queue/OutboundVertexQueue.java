package org.goldenorb.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.goldenorb.OrbPartitionCommunicationProtocol;
import org.goldenorb.Vertex;
import org.goldenorb.Vertices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboundVertexQueue {
	 private Logger omqLogger;
	
	 List<Vertices> partitionVertexMap;
	 private int numberOfPartitions;
	 private List<Integer> partitionVertexCounter;
	 private int maxVertices;
	 private Map<Integer, OrbPartitionCommunicationProtocol> orbClients;
	 private Class<? extends Vertex<?,?,?>> vertexClass;
	 private int partitionId;
	 
	 public OutboundVertexQueue(int numberOfPartitions, int maxVertices, Map<Integer, OrbPartitionCommunicationProtocol> orbClient, Class<? extends Vertex<?,?,?>> vertexClass, int partitionId){
		 omqLogger = LoggerFactory.getLogger(OutboundVertexQueue.class);
		 
		 this.numberOfPartitions = numberOfPartitions;
		 this.maxVertices = maxVertices;
		 this.orbClients = orbClient;
		 this.vertexClass = vertexClass;
		 this.partitionId = partitionId;
		 
		 partitionVertexMap = new ArrayList<Vertices>(numberOfPartitions);
		 for(int i = 0; i < numberOfPartitions; i++){
			 Vertices vs = new Vertices();
			 vs.setVertexType(vertexClass);
			 partitionVertexMap.add(vs);
		 }
		 
		 partitionVertexCounter = new ArrayList<Integer>(numberOfPartitions);
		 for(int i=0; i < numberOfPartitions; i++){
			 partitionVertexCounter.add(0);
		 }
	 }
	 
	 public void sendVertex(Vertex<?,?,?> v)
	 {
		int vertexHash = Math.abs(v.getVertexID().hashCode()) % numberOfPartitions ;
		Vertices currentPartition = partitionVertexMap.get(vertexHash);
		synchronized(currentPartition){
			Integer vertexCounter;
			synchronized(partitionVertexCounter){
				vertexCounter = partitionVertexCounter.get(vertexHash);
				vertexCounter++;
				partitionVertexCounter.set(vertexHash, vertexCounter);
				currentPartition.add(v);
				if (vertexCounter >= maxVertices){
						Vertices verticesToBeSent = currentPartition;
						Vertices vs = new Vertices();
						vs.setVertexType(vertexClass);
						
						omqLogger.info(this.toString() + " Partition: " + Integer.toString(partitionId)+ "Sending bulk messages. Count: " + vertexCounter + ", " + verticesToBeSent.size());
						verticesToBeSent.setVertexType(vertexClass);
						orbClients.get(vertexHash).sendVertices(verticesToBeSent);
						currentPartition = vs;
						partitionVertexCounter.set(vertexHash, new Integer(0));
				}
				partitionVertexMap.set(vertexHash, currentPartition);
			}
		} 
	}
	 
	 public void sendRemainingVertices(){
			 
		 for(int partitionID = 0; partitionID < numberOfPartitions; partitionID++){
			 omqLogger.info(this.toString() + " Partition: " + Integer.toString(partitionId)+ "Sending bulk messages. Count: " + partitionVertexCounter.get(partitionID) + ", " + partitionVertexMap.get(partitionID).size());
			 if(partitionVertexMap.get(partitionID) != null){
				 Vertices verticesToBeSent = partitionVertexMap.get(partitionID);
				 verticesToBeSent.setVertexType(vertexClass);
				 orbClients.get(partitionID).sendVertices(partitionVertexMap.get(partitionID));
			 }
		 } 
	 }
}
