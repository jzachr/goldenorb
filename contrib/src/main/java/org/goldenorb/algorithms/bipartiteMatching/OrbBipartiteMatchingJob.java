package org.goldenorb.algorithms.bipartiteMatching;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.goldenorb.OrbRunner;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.types.message.IntMessage;

public class OrbBipartiteMatchingJob extends OrbRunner{
	
	private OrbConfiguration orbConf;
	
	public static void main(String[] args){
		
		String inputpath = args[0];
		String outputpath = args[1];
		String totalpages = args[2];
		OrbBipartiteMatchingJob omvj = new OrbBipartiteMatchingJob();
		omvj.startJob(inputpath, outputpath, totalpages);
	}
	
	public void startJob(String inputPath, String outputPath, String totalPages){
		
		orbConf = new OrbConfiguration(true);
		
		orbConf.setFileInputFormatClass(TextInputFormat.class);
		orbConf.setFileOutputFormatClass(TextOutputFormat.class);
		orbConf.setVertexClass(BipartiteMatchingVertex.class);
		orbConf.setMessageClass(IntMessage.class);
		orbConf.setVertexInputFormatClass(BipartiteMatchingVertexReader.class);
		orbConf.setVertexOutputFormatClass(BipartiteMatchingVertexWriter.class);
		orbConf.setNumberOfMessageHandlers(10);
		orbConf.setNumberOfVertexThreads(10);
		orbConf.setNumberOfPartitionsPerMachine(4);
		
		// pass totalpages to all nodes
		
		runJob(orbConf);
		
	}
}
