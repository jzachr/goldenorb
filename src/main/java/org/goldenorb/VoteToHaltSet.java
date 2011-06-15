package org.goldenorb;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class VoteToHaltSet {
	private Set<String> vths;
	
	public VoteToHaltSet(Collection<String> initialVertices){
		vths = new HashSet<String>(initialVertices);
	}
	
	public VoteToHaltSet() {
		vths = new HashSet<String>();
	}

	public void addVertex(String vertexID){
			vths.add(vertexID);
	}
	
	public void voteToHalt(String vertexID){
		synchronized(vths){
			vths.remove(vertexID);
		}
	}
	
	public boolean isEmpty(){
		return vths.size() == 0;
	}
	
	public int size(){
		return vths.size();
	}
}
