package org.goldenorb.algorithms.singleSourceShortestPath;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.goldenorb.OrbRunner;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.types.message.IntMessage;

public class OrbSingleSourceShortestPathJob extends OrbRunner {

	public static final String USAGE = "usage: -i input-dir -o output-dir --reqpar requested-partitions --respar reserved-partitions -s source-vertex";
	public static final String ALGORITHM_NAME = "singlesourceshortestpath";
	public static final String SOURCE_VERTEX = ALGORITHM_NAME+".source";
	private static Options options;

	@SuppressWarnings("null")
	public static void main(String[] args) {
		
		// if (args.length != 1) {
		// System.out.println("Missing arguments");
		// System.out.println("usage: OrbSingleSourceShortestPathJob input-dir output-dir requested-partitions reserved-partitions source-vertex");
		// System.exit(-1);
		// }

//		Properties driverProperties = null;
//		String[] algorithmArgs = null;
//		parseCommandLine(args, driverProperties, algorithmArgs);

//		OrbSingleSourceShortestPathJob orbSingleSourceShortestPathJob = new OrbSingleSourceShortestPathJob();
//		orbSingleSourceShortestPathJob.initializeOptions();

//		CommandLine cmdLine = null;
//		try {
////			cmdLine = new GnuParser().parse(options, algorithmArgs);
//		} catch (ParseException e) {
//		}
//		;

//		String inDir = ""; //driverProperties.getProperty("mapred.input.dir");
//		String outDir = ""; //driverProperties.getProperty("mapred.output.dir");
//		int reqP = 0; //Integer.parseInt(driverProperties.getProperty("goldenOrb.orb.requestedPartitions"));
//		int resP = 0; //Integer.parseInt(driverProperties.getProperty("goldenOrb.orb.reservedPartitions"));
//
//		String src = "S"; // cmdLine.getOptionValue("s");

		OrbSingleSourceShortestPathJob job =  new OrbSingleSourceShortestPathJob();
		job.startJob(args);
	}

//	@SuppressWarnings("static-access")
//	private void addOption(Options options, String argName, String longOpt,
//			String description, boolean hasArg, boolean isRequired) {
//
//		options.addOption(OptionBuilder
//				.withArgName(argName)
//				.withDescription(description)
//				.withLongOpt(longOpt)
//				.hasArg(hasArg)
//				.isRequired(isRequired)
//				.create(argName));
//	}

	public void startJob(String[] args) {
		OrbConfiguration orbConf = getConf(true);

		orbConf.setFileInputFormatClass(TextInputFormat.class);
		orbConf.setFileOutputFormatClass(TextOutputFormat.class);
		orbConf.setVertexClass(SingleSourceShortestPathVertex.class);
		orbConf.setMessageClass(PathMessage.class);
		orbConf.setVertexInputFormatClass(SingleSourceShortestPathReader.class);
		orbConf.setVertexOutputFormatClass(SingleSourceShortestPathWriter.class);
		orbConf.setNumberOfMessageHandlers(10);
		orbConf.setNumberOfVertexThreads(10);

		// addressed in parseArgs
//		orbConf.setFileInputPath(inputPath);
//		orbConf.setFileOutputPath(outputPath);
//		orbConf.setOrbRequestedPartitions(requested);
//		orbConf.setOrbReservedPartitions(reserved);

		// addressed in parseArgs
//		orbConf.set(SOURCE_VERTEX, sourceVertex);

		
		try {
			parseArgs(orbConf, args, ALGORITHM_NAME);		
		} catch (IllegalArgumentException e) {
			printHelpMessage();
			System.exit(-1);
		}
		runJob(orbConf);
	}

	//	@Override
	protected void printHelpMessage() {
		System.out.println(USAGE);
	}

////	@Override
//	protected void initializeOptions() {
//		addOption(options, "s", "source-vertex", "Source Vertex.", true, true);
//	}
}
