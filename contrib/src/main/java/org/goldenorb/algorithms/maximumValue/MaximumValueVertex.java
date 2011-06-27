package org.goldenorb.algorithms.maximumValue;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.goldenorb.Edge;
import org.goldenorb.Vertex;
import org.goldenorb.types.message.IntMessage;

public class MaximumValueVertex extends Vertex<IntWritable, IntWritable, IntMessage>{
	
	int maxValue = 0;
	
	public MaximumValueVertex(){
		super(IntWritable.class, IntWritable.class, IntMessage.class);
	}

	public MaximumValueVertex(String _vertexID, IntWritable _value, List<Edge<IntWritable>> _edges) {
		super(_vertexID, _value, _edges);
	}

	@Override
	public void compute(Collection<IntMessage> messages) {
		
		int _maxValue = 0;
		
		for(IntMessage m: messages){
			
			int msgValue = ((IntWritable)m.getMessageValue()).get();
			if( msgValue > _maxValue ){
				_maxValue = msgValue;
			}
		}
		
		if(this.getValue().get() > _maxValue){
			_maxValue = this.getValue().get();
		}
		
		if(_maxValue > maxValue){
			maxValue = _maxValue;
			for(Edge<IntWritable> e: getEdges()){
				sendMessage(new IntMessage(e.getDestinationVertex(), new IntWritable(maxValue)));
			}
		}
		
		this.voteToHalt();
	}
	
	public int getMaxValue(){
		return maxValue;
	}
	
	@Override
	public String toString(){
		return "\"Value\":\"" + maxValue + "\"";
	}
}
