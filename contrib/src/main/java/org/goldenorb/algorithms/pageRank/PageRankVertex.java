package org.goldenorb.algorithms.pageRank;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.goldenorb.Edge;
import org.goldenorb.Vertex;
import org.goldenorb.algorithms.singleSourceShortestPath.SingleSourceShortestPathVertex;
import org.goldenorb.types.message.DoubleMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankVertex extends Vertex<DoubleWritable, IntWritable, DoubleMessage>{
	
	private final static Logger logger = LoggerFactory.getLogger(SingleSourceShortestPathVertex.class);
	
	// this should get passed in by the job
	int totalpages = 1000;
	int maxiterations = 10;
	double dampingfactor = 0.85;
	
	int currentiteration = 0;
	int outgoingEdgeCount = 0;
	
	public PageRankVertex(){
		super(DoubleWritable.class, IntWritable.class, DoubleMessage.class);
	}

	public PageRankVertex(String _vertexID, DoubleWritable _value, List<Edge<IntWritable>> _edges) {
		super(_vertexID, _value, _edges);
	}

	@Override
	public void compute(Collection<DoubleMessage> messages) {
		
		double _newrank = 0.0;
		double _outgoingrank;
		
		if( super.superStep() == 1 ) {
			
			try { 
				maxiterations = Integer.parseInt(super.getOci().getOrbProperty(OrbPageRankJob.MAXITERATIONS));
			} catch( Exception e ) {
				logger.debug("Max Iterations not set");
			}
			
			try { 
				totalpages = Integer.parseInt(super.getOci().getOrbProperty(OrbPageRankJob.TOTALPAGES));		
				if( totalpages == 0 ) {
					logger.debug("Total Pages not set");
					totalpages = 1000;
				}
			} catch( Exception e ) {
				logger.debug("Total Pages not set");
				totalpages = 1000;
			}
			
			try { 
				dampingfactor = Double.parseDouble(super.getOci().getOrbProperty(OrbPageRankJob.DAMPINGFACTOR));
			} catch( Exception e ) {
				logger.debug("Damping Factor not set");
			}
			outgoingEdgeCount = super.getEdges().size();
			super.setValue(new DoubleWritable(1.0/((double)totalpages)));
			if(outgoingEdgeCount == 0){
				outgoingEdgeCount = totalpages;
			}
			
			_outgoingrank = computeOutgoingRank(super.getValue().get());
			
			for(Edge<IntWritable> e: getEdges()){
				sendMessage(new DoubleMessage(e.getDestinationVertex(), new DoubleWritable(_outgoingrank)));
			}
			
		} else if (super.superStep() <= maxiterations) {
			
			for(DoubleMessage m: messages){
				
				double msgValue = ((DoubleMessage)m.getMessageValue()).get();
				_newrank += msgValue;
			}
			
			super.setValue(new DoubleWritable(_newrank));
			_outgoingrank = computeOutgoingRank(super.getValue().get());
			
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
		return super.getValue().get();
	}
	
	@Override
	public String toString(){
		return "\"PageRank\":\"" + super.getValue().get() + "\"";
	}
}
