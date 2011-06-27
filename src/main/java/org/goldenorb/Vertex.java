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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.io.Writable;

/**
 * An Abstract Class for a vertex. Vertex stores out bound edges. The vertex value, edge value, and message
 * value must extend Writable.
 * 
 * @param <VV>
 *          The class of the vertex value.
 * @param <EV>
 *          The class of the edge value.
 * @param <MV>
 *          The class of the message value.
 */

public abstract class Vertex<VV extends Writable,EV extends Writable,MV extends Message<? extends Writable>>
    implements Writable {
  
  private static OrbPartition.OrbCommunicationInterface oci;
  
  private String vertexID;
  private Collection<Edge<EV>> edges;
  private VV value;
  
  private Class<VV> vertexValue;
  private Class<EV> edgeValue;
  
  /**
   * Constructor
   * 
   */
  public Vertex() {}
  
  /**
   * Constructor
   * 
   * @param oci
   *          - OrbPartition.OrbCommunicationInterface
   * @param vertexID
   *          - unique String to represent the vertex
   * @param value
   *          - a Writable that is the vertex's value
   * @param edges
   *          - a Collection of the vertex's edges
   */
  public Vertex(OrbPartition.OrbCommunicationInterface oci,
                String vertexID,
                VV value,
                Collection<Edge<EV>> edges) {
    this.oci = oci;
    this.vertexID = vertexID;
    this.value = value;
    this.edges = edges;
  }
  
  /**
   * Constructor
   * 
   * @param vertexID
   *          - unique String to represent the vertex
   * @param value
   *          - a Writable that is the vertex's value
   * @param edges
   *          - a Collection of the vertex's edges
   */
  public Vertex(String vertexID, VV value, Collection<Edge<EV>> edges) {
    this.vertexID = vertexID;
    this.value = value;
    this.edges = edges;
  }
  
  /**
   * Constructor
   * 
   * @param OrbPartition
   *          .OrbCommunicationInterface oci
   * @param Class
   *          <VV> vertexValue
   * @param Class
   *          <EV> edgeValue
   * @param Class
   *          <MV> messageValue
   */
  public Vertex(OrbPartition.OrbCommunicationInterface oci,
                Class<VV> vertexValue,
                Class<EV> edgeValue,
                Class<MV> messageValue) {
    this.oci = oci;
    this.vertexValue = vertexValue;
    this.edgeValue = edgeValue;
  }
  
  /**
   * Constructor
   * 
   * @param Class
   *          <VV> vertexValue
   * @param Class
   *          <EV> edgeValue
   * @param Class
   *          <MV> messageValue
   */
  public Vertex(Class<VV> vertexValue, Class<EV> edgeValue, Class<MV> messageValue) {
    this.vertexValue = vertexValue;
    this.edgeValue = edgeValue;
  }
  
  /**
   * Gets the vertexID.
   * 
   * @returns String
   */
  public String vertexID() {
    return vertexID;
  }
  
  /**
   * 
   * @param messages
   */
  public abstract void compute(Collection<MV> messages);
  
  /**
   * Gets the superStep.
   * 
   * @returns long
   */
  protected long superStep() {
    return oci.superStep();
  }
  
  /**
   * Set the value.
   * 
   * @param value
   */
  protected void setValue(VV value) {
    this.value = value;
  }
  
  /**
   * Return the value.
   */
  public VV getValue() {
    return value;
  }
  
  /**
   * Return the edges.
   */
  protected Collection<Edge<EV>> getEdges() {
    return edges;
  }
  
  /**
   * 
   * @param message
   */
  public void sendMessage(MV message) {
    oci.sendMessage(message);
  }
  
  /**
   * Votes to halt.
   */
  public void voteToHalt() {
    oci.voteToHalt(vertexID);
  }
  
  /**
   * Return the oci.
   */
  public OrbPartition.OrbCommunicationInterface getOci() {
    return oci;
  }
  
  /**
   * Set the oci.
   * 
   * @param oci
   */
  public void setOci(OrbPartition.OrbCommunicationInterface oci) {
    this.oci = oci;
  }
  
  /**
   * 
   * @param DataInput
   *          in
   */
  public void readFields(DataInput in) throws IOException {
    vertexID = in.readUTF();
    
    value = (VV) newVertexValue();
    value.readFields(in);
    
    int numberOfEdges = in.readInt();
    ArrayList<Edge<EV>> _edges = new ArrayList<Edge<EV>>(numberOfEdges);
    for (int i = 0; i < numberOfEdges; i++) {
      Edge<EV> edge = new Edge<EV>(edgeValue);
      edge.readFields(in);
      _edges.add(edge);
    }
    
    edges = _edges;
  }
  
  /**
   * 
   * @param DataOutput
   *          out
   */
  public void write(DataOutput out) throws IOException {
    out.writeUTF(vertexID);
    value.write(out);
    out.writeInt(edges.size());
    for (Edge<EV> edge : edges) {
      edge.write(out);
    }
  }
  
  /**
   * 
   * @returns VV
   */
  protected VV newVertexValue() {
    VV vv = null;
    try {
      vv = vertexValue.newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    
    return vv;
  }
  
  @Override
  public String toString() {
    return "Vertex: " + vertexID + "  Contents: { " + value.toString() + " }";
  }
  
  /**
   * Return the vertexID.
   */
  public String getVertexID() {
    return vertexID;
  }
}
