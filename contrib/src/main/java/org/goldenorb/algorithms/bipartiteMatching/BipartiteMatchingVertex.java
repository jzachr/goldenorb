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
import org.goldenorb.util.message.IntSourceMessage;

public class BipartiteMatchingVertex extends
		Vertex<Text, IntWritable, IntSourceMessage> {

	final static int PHASE_0 = 0;
	final static int PHASE_1 = 1;
	final static int PHASE_2 = 2;
	final static int PHASE_3 = 3;
	final static int NUM_PHASES = 4;

	final static int GROUP_0 = 0;
	final static int GROUP_1 = 1;
	final static int NUM_GROUPS = 2;

	final static int ACCEPT = 1;
	final static int DENY = 0;
	final static int REQUEST = 1;

	// this should get passed in by the job
	int mygroup;

	public BipartiteMatchingVertex() {
		super(Text.class, IntWritable.class, IntSourceMessage.class);
	}

	public BipartiteMatchingVertex(String _vertexID, Text _value,
			List<Edge<IntWritable>> _edges) {
		super(_vertexID, _value, _edges);
		String[] values = _vertexID.toString().split(":");
		mygroup = Integer.parseInt(values[1]);
	}

	@Override
	public void compute(Collection<IntSourceMessage> messages) {

		switch (Integer.parseInt(Long.toString(superStep() - 1)) % NUM_PHASES) {
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

		this.voteToHalt();
	}

	private void phase0() {

		for (Edge<IntWritable> e : getEdges()) {
			sendMessage(new IntSourceMessage(this, e.getDestinationVertex(),
					new IntWritable(REQUEST)));
		}
	}

	private void phase1(Collection<IntSourceMessage> messages) {
		int _maxweight = 0;
		String _maxid = null;

		for (IntSourceMessage m : messages) {

			int msgValue = ((IntSourceMessage) m.getMessageValue()).get();
			String msgSource = ((IntSourceMessage) m.getMessageValue())
					.getSourceVertex();

			if (msgValue > _maxweight) {
				_maxweight = msgValue;
				// needs to pull sourceVertexId from message
				_maxid = msgSource;
			}

		}

		for (Edge<IntWritable> e : getEdges()) {
			if (e.getDestinationVertex().equals(_maxid))
				sendMessage(new IntSourceMessage(this,
						e.getDestinationVertex(), new IntWritable(ACCEPT)));
			else
				sendMessage(new IntSourceMessage(this,
						e.getDestinationVertex(), new IntWritable(DENY)));
		}
	}

	private void phase2(Collection<IntSourceMessage> messages) {
		int _maxweight = 0;
		String _maxid = null;

		for (IntSourceMessage m : messages) {

			int msgValue = ((IntSourceMessage) m.getMessageValue()).get();
			String msgSource = ((IntSourceMessage) m.getMessageValue())
					.getSourceVertex();

			if (msgValue > _maxweight) {
				_maxweight = msgValue;
				_maxid = msgSource;
			}

		}

		for (Edge<IntWritable> e : getEdges()) {
			if (e.getDestinationVertex().equals(_maxid)) {
				sendMessage(new IntSourceMessage(this,
						e.getDestinationVertex(), new IntWritable(ACCEPT)));
				this.setValue(new Text(_maxid));
			} else
				sendMessage(new IntSourceMessage(this,
						e.getDestinationVertex(), new IntWritable(DENY)));
		}
	}

	private void phase3(Collection<IntSourceMessage> messages) {
		int _maxweight = 0;
		String _maxid = null;

		for (IntSourceMessage m : messages) {

			int msgValue = ((IntSourceMessage) m.getMessageValue()).get();
			String msgSource = ((IntSourceMessage) m.getMessageValue())
					.getSourceVertex();

			_maxweight = msgValue;
			_maxid = msgSource;

		}

		this.setValue(new Text(_maxid));
	}

	@Override
	public String toString() {
		return "\"Matched Vertex\":\"" + this.getVertexID() + "\"";
	}
}
