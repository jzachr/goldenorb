package org.goldenorb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class ArrayListWritable<WritableType extends Writable> implements Writable {
	
	private ArrayList<WritableType> writables = new ArrayList<WritableType>(); 
	private Class<? extends WritableType> writableType;

	public int size(){
		return writables.size();
	}
	
	public ArrayListWritable(){
		
	}
	
	public ArrayList<WritableType> getArrayList(){
		return writables;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		String vertexClassName = in.readUTF();
		try {
			/*for(int i=0;i<1000;i++){
				System.out.println(vertexClassName);
			}*/
			writableType = (Class<WritableType>) Class.forName(vertexClassName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		int numberOfVertices = in.readInt();
		for(int i = 0; i < numberOfVertices; i++)
		{
			WritableType newVertex = null;
			try {
				newVertex = writableType.newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			newVertex.readFields(in);
			writables.add(newVertex);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(writableType.getName());
		
		out.writeInt(writables.size());
		
		for(WritableType vertexOut: writables)
		{
			vertexOut.write(out);
		}
	}
	

	public void setWritableType(Class<? extends WritableType> _vertexType)
	{
		writableType = _vertexType;
	}
	
	public void add(WritableType v)
	{
		writables.add(v);
	}

}
