package org.goldenorb.algorithms.semiclustering;

import java.util.Collection;

import org.goldenorb.Message;
import org.goldenorb.Vertex;
import org.goldenorb.types.ArrayListWritable;

public class SemiClusterMessage extends Message<SemiClusterWritable> {

	/**
	 * Licensed to Ravel, Inc. under one or more contributor license agreements.
	 * See the NOTICE file distributed with this work for additional information
	 * regarding copyright ownership. Ravel, Inc. licenses this file to you
	 * under the Apache License, Version 2.0 (the "License"); you may not use
	 * this file except in compliance with the License. You may obtain a copy of
	 * the License at
	 * 
	 * http://www.apache.org/licenses/LICENSE-2.0
	 * 
	 * Unless required by applicable law or agreed to in writing, software
	 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
	 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
	 * License for the specific language governing permissions and limitations
	 * under the License.
	 * 
	 */

	/**
	 * Constructor
	 * 
	 */
	public SemiClusterMessage() {
		super(SemiClusterWritable.class);
	}

	/**
	 * Constructor
	 * 
	 * @param destinationVertex
	 *            - String
	 * @param messageValue
	 *            - IntWritable
	 */
	public SemiClusterMessage(String destinationVertex, SemiClusterWritable messageValue) {
		super(SemiClusterWritable.class);
		this.setDestinationVertex(destinationVertex);
		this.setMessageValue(messageValue);
	}

	/**
	 * Return the primitive int value stored in the Writable.
	 */
	public double getScore() {
		return ((SemiClusterWritable) this.getMessageValue()).getScore().get();
	}

	/**
	 * Get vertices.
	 */
	public Collection<SemiClusteringVertex> getVertices() {
		return ((SemiClusterWritable) this.getMessageValue()).getVertices();
	}

	/**
	 * adds a vertex to this path
	 * 
	 * @param vertices
	 */
	public void addVertex(Vertex vertex) {
		((SemiClusterWritable)this.getMessageValue()).addVertex(vertex);
	}
}
