/**
 * Licensed to Ravel, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Ravel, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package org.goldenorb;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A set of Strings that represent the vertices.  Once the set is populated with
 * active vertices, vertices that want to 'vote to halt' remove themselves from the set.
 */

public class VoteToHaltSet {
	private Set<String> vths;
	
/**
 * Constructor
 *
 * @param  Collection<String> initialVertices A collection of vertexIDs
 */
	public VoteToHaltSet(Collection<String> initialVertices){
		vths = new HashSet<String>(initialVertices);
	}
	
/**
 * Constructor
 *
 */
	public VoteToHaltSet() {
		vths = new HashSet<String>();
	}

/**
 * 
 * @param  String vertexID
 */
	public void addVertex(String vertexID){
			vths.add(vertexID);
	}
	
/**
 * 
 * @param  String vertexID
 */
	public void voteToHalt(String vertexID){
		synchronized(vths){
			vths.remove(vertexID);
		}
	}
	
/**
 *
 * Return true if the set is empty 
 */
	public boolean isEmpty(){
		return vths.isEmpty();
	}
	
/**
 * Returns the number of elements in the set. 
 * @returns int
 */
	public int size(){
		return vths.size();
	}
	
/**
 * Clears the set.
 */
	public void clear(){
	  vths.clear();
	}
	
/**
 * Adds a collection of vertices to the set.
 * @param  Collection<String> vertices Collection of vertexIDs
 */
	public void addVertices(Collection<String> vertices){
	  vths.addAll(vertices);
	}
}
