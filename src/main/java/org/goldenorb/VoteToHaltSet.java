package org.goldenorb;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A set of Strings that represent 
 */

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
		return vths.isEmpty();
	}
	
	public int size(){
		return vths.size();
	}
	
	public void clear(){
	  vths.clear();
	}
	
	public void addVertices(Collection<String> vertices){
	  vths.addAll(vertices);
	}
}
