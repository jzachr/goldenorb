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

import org.apache.hadoop.io.Writable;

/**
 * A directed edge for a vertex.  The edge value must a extend Writable.
 * @param <EV> The class of the edge value.
 */

public class Edge<EV extends Writable> implements Writable {
  
  private String destinationVertex;
  private EV edgeValue;
  
  private Class<EV> edgeType;
  
/**
 * Constructor
 *
 * @param  Class<EV> edgeType
 */
  public Edge(Class<EV> edgeType) {
    this.edgeType = edgeType;
  }
  
/**
 * Constructor
 *
 * @param  String destinationVertex
 * @param  EV edgeValue
 */
  public Edge(String destinationVertex, EV edgeValue) {
    this.edgeValue = edgeValue;
    this.destinationVertex = destinationVertex;
  }
  
/**
 * Return the destinationVertex
 */
  public String getDestinationVertex() {
    return destinationVertex;
  }
  
/**
 * Set the destinationVertex
 * @param  String destinationVertex
 */
  public void setDestinationVertex(String destinationVertex) {
    this.destinationVertex = destinationVertex;
  }
  
/**
 * Return the edgeValue
 */
  public EV getEdgeValue() {
    return edgeValue;
  }
  
/**
 * Set the edgeValue
 * @param  EV edgeValue
 */
  public void setEdgeValue(EV edgeValue) {
    this.edgeValue = edgeValue;
  }
  
/**
 * Deserialize the fields of this object from in.
 * @param  DataInput in DataInput to deseriablize this object from.
 */
  public void readFields(DataInput in) throws IOException {
    destinationVertex = in.readUTF();
    try {
      edgeValue = (EV) edgeType.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    edgeValue.readFields(in);
  }
  
/**
 * Serialize the fields of this object to out.
 * @param  DataOutput out  DataOuput to serialize this object into.
 */
  public void write(DataOutput out) throws IOException {
    out.writeUTF(destinationVertex);
    edgeValue.write(out);
  }
}
