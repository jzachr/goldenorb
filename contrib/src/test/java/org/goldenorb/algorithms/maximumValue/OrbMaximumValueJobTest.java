package org.goldenorb.algorithms.maximumValue;

public class OrbMaximumValueJobTest {
	public static void main(String[] args) {
		OrbMaximumValueJob omvj = new OrbMaximumValueJob();

		String testInput = "contrib/src/test/resources/testmaxvaluein.txt";
		String testOutput = "contrib/src/test/resources/testmaxvalueout.txt";

		omvj.startJob(testInput, testOutput);

	}
}
