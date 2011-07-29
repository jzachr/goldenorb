package org.goldenorb.algorithms.singleSourceShortestPath;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.goldenorb.OrbRunner;
import org.goldenorb.conf.OrbConfiguration;

public class OrbSingleSourceShortestPathJob extends OrbRunner {

	public static final String USAGE = "mapred.input.dir=/home/user/input/ mapred.output.dir=/home/user/output/ goldenOrb.orb.requestedPartitions=10 goldenOrb.orb.reservedPartitions=10 [goldenOrb.orb.partitionsPerMachine=2] [goldenOrb.job.number=3] [goldenOrb.job.name=\"SSSP JOB\"] sssp.startnode=\"Node0\"";
	public static final String ALGORITHM_NAME = "singlesourceshortestpath";
	public static final String SOURCE_VERTEX = ALGORITHM_NAME+".source";

	public static void main(String[] args) {
		OrbSingleSourceShortestPathJob job =  new OrbSingleSourceShortestPathJob();
		job.startJob(args);
	}

	public void startJob(String[] args) {
		OrbConfiguration orbConf = new OrbConfiguration(true);

		orbConf.setFileInputFormatClass(TextInputFormat.class);
		orbConf.setFileOutputFormatClass(TextOutputFormat.class);
		orbConf.setVertexClass(SingleSourceShortestPathVertex.class);
		orbConf.setMessageClass(PathMessage.class);
		orbConf.setVertexInputFormatClass(SingleSourceShortestPathReader.class);
		orbConf.setVertexOutputFormatClass(SingleSourceShortestPathWriter.class);
		orbConf.setNumberOfMessageHandlers(10);
		orbConf.setNumberOfVertexThreads(10);

		try {
			parseArgs(orbConf, args, ALGORITHM_NAME);		
		} catch (Exception e) {
			printHelpMessage();
			System.exit(-1);
		}
		runJob(orbConf);
	}

	@Override
	public void printHelpMessage() {
		super.printHelpMessage();
		System.out.println(USAGE);
	}
}
