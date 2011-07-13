package org.goldenorb.algorithms.pageRank;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.goldenorb.OrbRunner;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.types.message.DoubleMessage;

public class OrbPageRankJob extends OrbRunner {

	public static String TOTALPAGES = "pagerank.totalpages";
	public static String MAXITERATIONS = "pagerank.maxiterations";
	public static String DAMPINGFACTOR = "semiclustering.boundaryfactor";

	private OrbConfiguration orbConf;

	public static void main(String[] args) {
		if (args.length != 8) {
			System.out.println("Missing arguments");
			System.out
					.println("usage: OrbPageRankJob input-dir output-dir totalpages maxiterations dampingfactor requested-partitions reserved-partitions classpath");
			System.exit(-1);
		}
		String inputpath = args[0];
		String outputpath = args[1];
		String totalpages = args[2];
		String maxiterations = args[3];
		String dampingfactor = args[4];
		int reqP = Integer.parseInt(args[5]);
		int resP = Integer.parseInt(args[6]);
		String cp = args[7];
		OrbPageRankJob oprj = new OrbPageRankJob();
		oprj.startJob(inputpath, outputpath, totalpages, maxiterations,
				dampingfactor, reqP, resP, cp);
	}

	public void startJob(String inputPath, String outputPath,
			String totalPages, String maxiterations, String dampingfactor, int requested, int reserved, String classPath) {

		orbConf = new OrbConfiguration(true);

		orbConf.setFileInputFormatClass(TextInputFormat.class);
		orbConf.setFileOutputFormatClass(TextOutputFormat.class);
		orbConf.setVertexClass(PageRankVertex.class);
		orbConf.setMessageClass(DoubleMessage.class);
		orbConf.setVertexInputFormatClass(PageRankVertexReader.class);
		orbConf.setVertexOutputFormatClass(PageRankVertexWriter.class);
		orbConf.setNumberOfMessageHandlers(10);
		orbConf.setNumberOfVertexThreads(10);
		
		orbConf.setFileInputPath(inputPath);
		orbConf.setFileOutputPath(outputPath);
		
		orbConf.setOrbRequestedPartitions(requested);
		orbConf.setOrbReservedPartitions(reserved);
		
		orbConf.setOrbClassPaths(classPath);
		
		orbConf.set(TOTALPAGES, totalPages);
		orbConf.set(MAXITERATIONS, maxiterations);
		orbConf.set(DAMPINGFACTOR, dampingfactor);

		runJob(orbConf);

	}
}
