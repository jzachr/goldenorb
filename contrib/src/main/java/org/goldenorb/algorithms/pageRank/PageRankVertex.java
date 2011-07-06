package org.goldenorb.algorithms.pageRank;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.goldenorb.Edge;
import org.goldenorb.Vertex;
import org.goldenorb.algorithms.semiclustering.OrbSemiClusteringJob;
import org.goldenorb.types.message.DoubleMessage;
import org.goldenorb.types.message.IntMessage;

public class PageRankVertex extends Vertex<DoubleWritable, IntWritable, DoubleMessage>{
	
	// this should get passed in by the job
	int totalpages = 1000;
	int maxiterations = 10;
	double dampingfactor = 0.85;
	
	int currentiteration = 0;
	int outgoingEdgeCount = 0;
	
	double pageRank = Double.NaN;
	
	public PageRankVertex(){
		super(DoubleWritable.class, IntWritable.class, DoubleMessage.class);
	}

	public PageRankVertex(String _vertexID, DoubleWritable _value, List<Edge<IntWritable>> _edges) {
		super(_vertexID, _value, _edges);
		
		try { 
			maxiterations = Integer.parseInt(super.getOci().getOrbProperty(OrbPageRankJob.MAXITERATIONS));
		} catch( Exception e ) {
			
		}
		
		try { 
			totalpages = Integer.parseInt(super.getOci().getOrbProperty(OrbPageRankJob.TOTALPAGES));		} catch( Exception e ) {
			
		}
		
		try { 
			dampingfactor = Double.parseDouble(super.getOci().getOrbProperty(OrbPageRankJob.DAMPINGFACTOR));
		} catch( Exception e ) {
			
		}
		outgoingEdgeCount = _edges.size();
		pageRank = 1.0/((double)totalpages);
		if(outgoingEdgeCount == 0){
			outgoingEdgeCount = totalpages;
		}
	}

	@Override
	public void compute(Collection<DoubleMessage> messages) {
		
		double _newrank = 0;
		double _outgoingrank;
		
		for(DoubleMessage m: messages){
			
			double msgValue = ((DoubleMessage)m.getMessageValue()).get();
			_newrank += msgValue;
		}
		
		pageRank = _newrank;
		
		_outgoingrank = computeOutgoingRank(_newrank);
		
		if(currentiteration <= maxiterations){
			for(Edge<IntWritable> e: getEdges()){
				sendMessage(new DoubleMessage(e.getDestinationVertex(), new DoubleWritable(_outgoingrank)));
			}
		}
		
		this.voteToHalt();
	}
	
	private double computeOutgoingRank(double _newrank) {
		double outPR = 0.0;
		//outPR = _newrank/((double) outgoingEdgeCount);
		// correction to account for likelihood of loops
		outPR = dampingfactor * (_newrank/((double) outgoingEdgeCount)) + (1-dampingfactor)*(_newrank/((double) totalpages));
		return outPR;
	}

	public double getPageRank(){
		return pageRank;
	}
	
	@Override
	public String toString(){
		return "\"PageRank\":\"" + pageRank + "\"";
	}
}
