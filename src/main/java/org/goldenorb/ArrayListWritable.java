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
 * @param <WRITABLE_TYPE>
 */

public class ArrayListWritable<WRITABLE_TYPE extends Writable> implements Writable {
  
  private ArrayList<WRITABLE_TYPE> writables = new ArrayList<WRITABLE_TYPE>();
  private Class<? extends WRITABLE_TYPE> writableType;
  
  public int size() {
    return writables.size();
  }
  
  public ArrayListWritable() {}
  
  public ArrayList<WRITABLE_TYPE> getArrayList() {
    return writables;
  }
  
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
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(writableType.getName());
    out.writeInt(writables.size());
    for (WRITABLE_TYPE vertexOut : writables) {
      vertexOut.write(out);
    }
  }
  
  public void setWritableType(Class<? extends WRITABLE_TYPE> _vertexType) {
    writableType = _vertexType;
  }
  
  public void add(WRITABLE_TYPE v) {
    writables.add(v);
  }
  
}
