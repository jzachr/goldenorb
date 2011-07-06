package org.goldenorb.algorithms.semiclustering;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.goldenorb.Edge;
import org.goldenorb.Vertex;
import org.goldenorb.types.ArrayListWritable;
import org.goldenorb.types.message.DoubleMessage;

public class SemiClusteringVertex extends Vertex<ArrayListWritable, IntWritable, SemiClusterMessage>{
	
	final static int FIRST_SUPERSTEP = 1;
	
	// this should get passed in by the job
	int vmax = 100;
	int cmax = 10;
	int maxiterations = 10;
	double boundaryedgefactor = 0.5;
	
	public SemiClusteringVertex(){
		super(ArrayListWritable.class, IntWritable.class, SemiClusterMessage.class);
	}

	public SemiClusteringVertex(String _vertexID, ArrayListWritable<SemiClusterWritable> _value, List<Edge<IntWritable>> _edges) {
		super(_vertexID, null, _edges);
		ArrayListWritable<SemiClusterWritable> _array = new ArrayListWritable<SemiClusterWritable>();
		_array.setWritableType(SemiClusterWritable.class);
		this.setValue(_array);
		
		try { 
			maxiterations = Integer.parseInt(super.getOci().getOrbProperty(OrbSemiClusteringJob.MAXITERATIONS));
		} catch( Exception e ) {
			
		}
		
		try { 
			vmax = Integer.parseInt(super.getOci().getOrbProperty(OrbSemiClusteringJob.MAXVERTICES));
		} catch( Exception e ) {
			
		}
		
		try { 
			cmax = Integer.parseInt(super.getOci().getOrbProperty(OrbSemiClusteringJob.MAXCLUSTERS));
		} catch( Exception e ) {
			
		}
		
		try { 
			boundaryedgefactor = Double.parseDouble(super.getOci().getOrbProperty(OrbSemiClusteringJob.BOUNDARYFACTOR));
		} catch( Exception e ) {
			
		}
	}

	@Override
	public void compute(Collection<SemiClusterMessage> messages) {
		
		double _newrank = 0;
		double _outgoingrank;
		
		if( this.superStep() == FIRST_SUPERSTEP ) {
			SemiClusterWritable _self = new SemiClusterWritable();
			_self.addVertex(this);
			_self.setScore(new DoubleWritable(computeSemiClusterScore(_self)));
			for(Edge<IntWritable> e: getEdges()){
				sendMessage(new SemiClusterMessage(e.getDestinationVertex(), _self));
			}
			
			this.voteToHalt();
		} 
		
		if( this.superStep() == maxiterations+1 ) {
			this.voteToHalt();
		}
		
		TreeSet<SemiClusterWritable> _candidates = new TreeSet<SemiClusterWritable>();
		for(SemiClusterMessage m: messages){
			
			SemiClusterWritable _cluster = (SemiClusterWritable)m.getMessageValue();
			double msgValue = _cluster.getScore().get();
			
			_candidates.add(_cluster);
			
			if( 	_cluster.getVertexCount() == vmax 
				&& !_cluster.getVertexIds().contains(this.getVertexID())) {
				
				Configuration conf = new Configuration();
				SemiClusterWritable plusself = WritableUtils.clone(_cluster, conf);
				plusself.addVertex(this);
				plusself.setScore(new DoubleWritable(computeSemiClusterScore(plusself)));
				_candidates.add(plusself);
				
			}
		}
		
		Iterator<SemiClusterWritable> bestcandidates = _candidates.descendingIterator();
		
		ArrayListWritable<SemiClusterWritable> _array = new ArrayListWritable<SemiClusterWritable>();
		_array.setWritableType(SemiClusterWritable.class);
		int _cindex = 0; 
		while( bestcandidates.hasNext() && _cindex < cmax ) {
			SemiClusterWritable candidate = bestcandidates.next();
			for(Edge<IntWritable> e: getEdges()){
				sendMessage(new SemiClusterMessage(e.getDestinationVertex(), candidate));
			}
			if( candidate.getVertexIds().contains(this.vertexID()))
				_array.add(candidate);
		}
		
		this.voteToHalt();
	}
	
	private double computeSemiClusterScore(SemiClusterWritable cluster) {
		double score = 0.0;
		//outPR = _newrank/((double) outgoingEdgeCount);
		// correction to account for likelihood of loops
		if( cluster.getVertexCount() <= 1 ) {
			score = 1.0;
		}
		else {	
			score = 	(cluster.getInternalWeight() - boundaryedgefactor * cluster.getBoundaryWeight())
					/	(cluster.getVertexCount()*(cluster.getVertexCount()-1)/2);
		}
		return score;
	}

	@Override
	public String toString(){
		return "\"Candidates\":\"" + this.getValue().toString() + "\"";
	}
}
