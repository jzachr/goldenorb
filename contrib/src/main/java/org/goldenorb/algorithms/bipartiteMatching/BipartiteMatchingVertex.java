package org.goldenorb.algorithms.bipartiteMatching;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.goldenorb.Edge;
import org.goldenorb.Vertex;
import org.goldenorb.types.message.DoubleMessage;
import org.goldenorb.types.message.IntMessage;

public class BipartiteMatchingVertex extends
		Vertex<Text, IntWritable, IntMessage> {

	static int PHASE_0 = 0;
	static int PHASE_1 = 1;
	static int PHASE_2 = 2;
	static int PHASE_3 = 3;
	static int NUM_PHASES = 4;

	static int GROUP_0 = 0;
	static int GROUP_1 = 1;
	static int NUM_GROUPS = 2;

	static int ACCEPT = 1;
	static int DENY = 0;

	// this should get passed in by the job
	int mygroup;
	int currentiteration = 0;

	public BipartiteMatchingVertex() {
		super(Text.class, IntWritable.class, IntMessage.class);
	}

	public BipartiteMatchingVertex(String _vertexID, Text _value,
			List<Edge<IntWritable>> _edges) {
		super(_vertexID, _value, _edges);
		String[] values = _vertexID.toString().split(":");
		mygroup = Integer.parseInt(values[1]);
	}

	@Override
	public void compute(Collection<IntMessage> messages) {
/*
		switch (currentiteration % NUM_PHASES) {
		case PHASE_0:
			switch (mygroup) {
			case GROUP_0:
				if (this.getValue().equals(null))
					phase0();
			}
			break;
		case PHASE_1:
			switch (mygroup) {
			case GROUP_1:
				if (this.getValue().equals(null))
					phase1(messages);
				break;
			}
		case PHASE_2:
			switch (mygroup) {
			case GROUP_0:
				if (this.getValue().equals(null))
					phase2(messages);
			}
			break;
		case PHASE_3:
			switch (mygroup) {
			case GROUP_1:
				if (this.getValue().equals(null))
					phase3(messages);
				break;
			}
		}
*/
		this.voteToHalt();
	}

	private void phase0() {
		int _maxweight = 0;
		String _maxid;

		/*for (Edge<IntWritable> e : getEdges()) {
			if (e.getDestinationVertex().equals(_maxid))
				sendMessage(new IntMessage(e.getDestinationVertex(),
						new IntWritable(ACCEPT)));
			else
				sendMessage(new IntMessage(e.getDestinationVertex(),
						new IntWritable(DENY)));
		}*/
	}

	private void phase1(Collection<IntMessage> messages) {
		int _maxweight = 0;
		String _maxid;

		for (IntMessage m : messages) {

			int msgValue = ((IntMessage) m.getMessageValue()).get();

			if (msgValue > _maxweight) {
				_maxweight = msgValue;
				// needs to pull sourceVertexId from message
				// _maxid =
			}

		}

		/*for (Edge<IntWritable> e : getEdges()) {
			if (e.getDestinationVertex().equals(_maxid))
				sendMessage(new IntMessage(e.getDestinationVertex(),
						new IntWritable(ACCEPT)));
			else
				sendMessage(new IntMessage(e.getDestinationVertex(),
						new IntWritable(DENY)));
		}*/
	}

	private void phase2(Collection<IntMessage> messages) {
		int _maxweight = 0;
		String _maxid;

		for (IntMessage m : messages) {

			int msgValue = ((IntMessage) m.getMessageValue()).get();

			if (msgValue > _maxweight) {
				_maxweight = msgValue;
				// needs to pull sourceVertexId from message
				// _maxid =
			}

		}

//		for (Edge<IntWritable> e : getEdges()) {
//			if (e.getDestinationVertex().equals(_maxid)) {
//				sendMessage(new IntMessage(e.getDestinationVertex(),
//						new IntWritable(ACCEPT)));
//				this.setValue(new Text(_maxid));
//			} else
//				sendMessage(new IntMessage(e.getDestinationVertex(),
//						new IntWritable(DENY)));
//		}
	}

	private void phase3(Collection<IntMessage> messages) {
		int _maxweight = 0;
		String _maxid;

		for (IntMessage m : messages) {

			int msgValue = ((IntMessage) m.getMessageValue()).get();
			_maxweight = msgValue;
				// needs to pull sourceVertexId from message
				// _maxid =

		}

		//this.setValue(_maxid);
	}

	// sendMessage(new IntMessage(1)

	@Override
	public String toString() {
		return "\"Matched Vertex\":\"" + this.getVertexID() + "\"";
	}
}
