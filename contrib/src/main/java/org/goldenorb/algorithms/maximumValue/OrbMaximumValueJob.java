package org.goldenorb.algorithms.maximumValue;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.goldenorb.OrbRunner;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.types.message.IntMessage;

public class OrbMaximumValueJob extends OrbRunner{
	
	private OrbConfiguration orbConf;
	
	public static void main(String[] args){
		
		String inputpath = args[0];
		String outputpath = args[1];
		OrbMaximumValueJob omvj = new OrbMaximumValueJob();
		omvj.startJob(inputpath, outputpath);
	}
	
	public void startJob(String inputPath, String outputPath){
		
		orbConf = new OrbConfiguration(true);
		
		orbConf.setFileInputFormatClass(TextInputFormat.class);
		orbConf.setFileOutputFormatClass(TextOutputFormat.class);
		orbConf.setVertexClass(MaximumValueVertex.class);
		orbConf.setMessageClass(IntMessage.class);
		orbConf.setVertexInputFormatClass(MaximumValueVertexReader.class);
		orbConf.setVertexOutputFormatClass(MaximumValueVertexWriter.class);
		orbConf.setNumberOfMessageHandlers(10);
		orbConf.setNumberOfVertexThreads(10);
		orbConf.setNumberOfPartitionsPerMachine(4);
		runJob(orbConf);
		
	}
}
