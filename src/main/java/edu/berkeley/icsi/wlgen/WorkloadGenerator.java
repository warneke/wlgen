package edu.berkeley.icsi.wlgen;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public final class WorkloadGenerator {

	private final String inputDir;

	private final boolean generateInput;

	private final int mapLimit;

	private final int reduceLimit;

	private final int jobLimit;

	private WorkloadGenerator(final String inputDir, final boolean generateInput, final int mapLimit,
			final int reduceLimit, final int jobLimit) {

		this.inputDir = inputDir;
		this.generateInput = generateInput;
		this.mapLimit = mapLimit;
		this.reduceLimit = reduceLimit;
		this.jobLimit = jobLimit;
	}

	private void loadWorkloadTraces() throws IOException {

		final MapReduceWorkload mrw = MapReduceWorkload.reconstructFromTraces();

	}

	public static void main(final String[] args) throws IOException {

		final Options options = new Options();
		options.addOption("i", "input", true, "Specifies the input directory containing the traces");
		options.addOption("g", "generate", false, "Generate the input files before running the jobs");
		options.addOption("m", "map", true, "Only run jobs with less than the specified number of map tasks");
		options.addOption("r", "reduce", true, "Only run jobs with less than the specified number of reduce tasks");
		options.addOption("l", "limit", true, "Limit the number of jobs to run to the specified value");

		String inputDir = null;
		boolean generateInput = false;
		int mapLimit = Integer.MAX_VALUE;
		int reduceLimit = Integer.MAX_VALUE;
		int jobLimit = Integer.MAX_VALUE;

		final CommandLineParser parser = new PosixParser();
		final CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			return;
		}

		if (!cmd.hasOption("i")) {
			final HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("wlgen", options);
			return;
		}

		inputDir = cmd.getOptionValue("i");

		if (cmd.hasOption("g")) {
			generateInput = Boolean.parseBoolean(cmd.getOptionValue("g"));
		}

		if (cmd.hasOption("m")) {
			mapLimit = Integer.parseInt(cmd.getOptionValue("m"));
		}

		if (cmd.hasOption("r")) {
			reduceLimit = Integer.parseInt(cmd.getOptionValue("r"));
		}

		if (cmd.hasOption("l")) {
			reduceLimit = Integer.parseInt(cmd.getOptionValue("l"));
		}

		final WorkloadGenerator wlg = new WorkloadGenerator(inputDir, generateInput, mapLimit, reduceLimit, jobLimit);
		wlg.loadWorkloadTraces();
	}
}
