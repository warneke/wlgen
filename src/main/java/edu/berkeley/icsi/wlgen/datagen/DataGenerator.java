package edu.berkeley.icsi.wlgen.datagen;

import java.io.IOException;
import java.net.InetSocketAddress;

import edu.berkeley.icsi.wlgen.File;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.util.JarFileCreator;

public class DataGenerator {

	static final String FILE_SIZE = "file.size";

	private static final int BLOCK_SIZE = 64 * 1024 * 1024; // 64 MB

	private static final int SUBTASKS_PER_INSTANCE = 4;

	private final String jobManagerAddress;

	private final String basePath;

	private final JobGraph jobGraph;

	private final JobInputVertex inputVertex;

	private final JobFileOutputVertex outputVertex;

	public DataGenerator(final String jobManagerAddress, final String basePath) throws IOException {

		this.jobManagerAddress = jobManagerAddress;
		this.basePath = basePath;

		this.jobGraph = new JobGraph("Data Generator");

		this.inputVertex = new JobInputVertex("Input", this.jobGraph);
		this.inputVertex.setInputClass(DataGeneratorInput.class);
		this.inputVertex.setNumberOfSubtasksPerInstance(SUBTASKS_PER_INSTANCE);

		this.outputVertex = new JobFileOutputVertex("Output", this.jobGraph);
		this.outputVertex.setFileOutputClass(DataGeneratorOutput.class);
		this.outputVertex.setNumberOfSubtasksPerInstance(SUBTASKS_PER_INSTANCE);

		this.outputVertex.setVertexToShareInstancesWith(this.inputVertex);

		try {
			this.inputVertex.connectTo(this.outputVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
		} catch (JobGraphDefinitionException e) {
			throw new IOException(e);
		}

		final java.io.File jarFile = java.io.File.createTempFile("datagen", ".jar");
		jarFile.deleteOnExit();

		final JarFileCreator jfc = new JarFileCreator(jarFile);
		jfc.addClass(DataGeneratorInput.class);
		jfc.addClass(DataGeneratorOutput.class);

		jfc.createJarFile();

		this.jobGraph.addJar(new Path("file://" + jarFile.getAbsolutePath()));
	}

	public void generate(final File inputFile) throws IOException {

		final int numberOfSubtasks = (int) ((inputFile.getSize() + (BLOCK_SIZE - 1)) / BLOCK_SIZE);

		this.inputVertex.setNumberOfSubtasks(numberOfSubtasks);
		this.outputVertex.setNumberOfSubtasks(numberOfSubtasks);

		this.outputVertex.setFilePath(new Path(this.basePath, inputFile.getName()));
		this.outputVertex.getConfiguration().setLong(FILE_SIZE, inputFile.getSize());

		final InetSocketAddress isa = new InetSocketAddress(jobManagerAddress,
			ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

		final JobClient jobClient = new JobClient(this.jobGraph, new Configuration(), isa);

		try {
			jobClient.submitJobAndWait();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}
