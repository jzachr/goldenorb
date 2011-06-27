package org.goldenorb.algorithms.singleSourceShortestPath;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.goldenorb.Edge;
import org.goldenorb.Message;
import org.goldenorb.Vertex;
import org.goldenorb.types.message.IntMessage;

public class SingleSourceShortestPathVertex extends Vertex<PathWritable, IntWritable, PathMessage> {
	
	PathWritable shortestPath;
	
	public SingleSourceShortestPathVertex(){
		super(PathWritable.class, IntWritable.class, PathMessage.class);
	}

	public SingleSourceShortestPathVertex(String _vertexID, PathWritable _value, List<Edge<IntWritable>> _edges) {
		super(_vertexID, _value, _edges);
	}

	@Override
	public void compute(Collection<PathMessage> messages) {
		
		int _minWeight = shortestPath.getWeight().get();
		PathWritable _path = null;
		
		for(PathMessage m: messages){
			
			int msgValue = ((PathWritable)m.getMessageValue()).getWeight().get();
			if( msgValue < _minWeight ){
				_minWeight = msgValue;
				_path = ((PathWritable)m.getMessageValue());
			}
		}
		
		if(_minWeight < shortestPath.getWeight().get()) {
			shortestPath = _path;
			PathWritable _outpath = null;
			try {
				ReflectionUtils.cloneWritableInto(_outpath, shortestPath);
			} catch( Exception e ) {
				e.printStackTrace();
			}
			_outpath.addVertex(this);
			for(Edge<IntWritable> e: getEdges()){
				_outpath.setWeight(new IntWritable(shortestPath.getWeight().get()+e.getEdgeValue().get()));
				sendMessage(new PathMessage(e.getDestinationVertex(), _outpath));
			}
		}
		
		this.voteToHalt();
	}
	
	public int getShortestPath(){
		return shortestPath.getWeight().get();
	}
	
	@Override
	public String toString(){
		String out = new String("\"Distance\":\"" + shortestPath.getWeight().get() + "\"");
		out += shortestPath.getVertices().toString();
		return out;
	}
}
