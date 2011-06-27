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

import org.apache.hadoop.io.Writable;

/**
 * A Writable for ArrayList containing instances of a class.  The The elements of this writable must all be 
 * instances of the same class and extend Writable.  
 *
 * @param <WRITABLE_TYPE> The class of the Writable that will populate the ArrayList
 */

public class ArrayListWritable<WRITABLE_TYPE extends Writable> implements Writable {
  
  private ArrayList<WRITABLE_TYPE> writables = new ArrayList<WRITABLE_TYPE>();
  private Class<? extends WRITABLE_TYPE> writableType;
  
/**
 * Returns the number of elements in the ArrayList.
 * @returns int
 */
  public int size() {
    return writables.size();
  }
  
/**
 * Default Constructor
 *
 */
  public ArrayListWritable() {}
  
/**
 * Return the ArrayList that is holding the Writables.
 */
  public ArrayList<WRITABLE_TYPE> getArrayList() {
    return writables;
  }
  
/**
 * Deserialize the fields of this object from in.
 * @param  in - DataInput to deserialize this object from
 */
  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
    
    String vertexClassName = in.readUTF();
    try {
      writableType = (Class<WRITABLE_TYPE>) Class.forName(vertexClassName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    
    int numberOfVertices = in.readInt();
    for (int i = 0; i < numberOfVertices; i++) {
      WRITABLE_TYPE newVertex = null;
      try {
        newVertex = writableType.newInstance();
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      newVertex.readFields(in);
      writables.add(newVertex);
    }
  }
  
/**
 * Serialize the fields of the object to out.
 * @param  DataOutput out DataOuput to serialize this object into.
 */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(writableType.getName());
    out.writeInt(writables.size());
    for (WRITABLE_TYPE vertexOut : writables) {
      vertexOut.write(out);
    }
  }
  
/**
 * Set the writableType
 * @param  Class<? extends WRITABLE_TYPE> _vertexType
 */
  public void setWritableType(Class<? extends WRITABLE_TYPE> _vertexType) {
    writableType = _vertexType;
  }
  
/**
 * Adds Writable to the array.
 * @param  WRITABLE_TYPE v
 */
  public void add(WRITABLE_TYPE v) {
    writables.add(v);
  }
  
}
