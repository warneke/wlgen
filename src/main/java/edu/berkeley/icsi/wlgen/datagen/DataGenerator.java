package edu.berkeley.icsi.wlgen.datagen;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

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

	private final JobClient jobClient;

	private JobInputVertex inputVertex;

	private JobFileOutputVertex outputVertex;

	public DataGenerator(final String jobManagerAddress) throws IOException {

		final JobGraph jobGraph = new JobGraph("Data Generator");

		this.inputVertex = new JobInputVertex("Input", jobGraph);
		this.inputVertex.setInputClass(DataGeneratorInput.class);

		this.outputVertex = new JobFileOutputVertex("Output", jobGraph);
		this.outputVertex.setFileOutputClass(DataGeneratorOutput.class);

		this.outputVertex.setVertexToShareInstancesWith(this.inputVertex);

		try {
			this.inputVertex.connectTo(this.outputVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
		} catch (JobGraphDefinitionException e) {
			throw new IOException(e);
		}

		final File jarFile = File.createTempFile("dg", ".jar");
		jarFile.deleteOnExit();

		final JarFileCreator jfc = new JarFileCreator(jarFile);
		jfc.addClass(DataGeneratorInput.class);
		jfc.addClass(DataGeneratorOutput.class);

		jfc.createJarFile();

		final InetSocketAddress isa = new InetSocketAddress(jobManagerAddress,
			ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

		this.jobClient = new JobClient(jobGraph, new Configuration(), isa);
	}

	public void generate(final Path basePath, final File inputFile) {

	}

	public void shutdown() {

		this.jobClient.close();
	}
}
