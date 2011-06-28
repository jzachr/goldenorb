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
 */
package org.goldenorb;

/**
 * This class is used to aggregate Vertex objects and is an extension of ArrayListWritable, where most of the
 * methods and data structures for Vertices exist.
 * 
 */
@SuppressWarnings("rawtypes")
public class Vertices extends ArrayListWritable<Vertex> {
  public Vertices(){}
  /**
   * Creates a Vertices object and sets its type to the given Vertex class.
   * @param vertexType
   */
  public Vertices(Class<? extends Vertex> vertexType) {
    this.setWritableType(vertexType);
  }
  
  /**
   * Sets the vertexType of the Vertices object to the given Vertex class.
   * @param vertexType
   */
  public void setVertexType(Class<? extends Vertex> vertexType) {
    this.setWritableType(vertexType);
  }
}
