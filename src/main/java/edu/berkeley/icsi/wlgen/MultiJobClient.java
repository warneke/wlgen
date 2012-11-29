package edu.berkeley.icsi.wlgen;

import java.io.IOException;
import java.net.InetSocketAddress;

import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.protocols.JobManagementProtocol;
import eu.stratosphere.nephele.rpc.CommonTypeUtils;
import eu.stratosphere.nephele.rpc.RPCService;

public class MultiJobClient {

	private final RPCService rpcService;

	private final JobManagementProtocol jobManagementProtocol;

	public MultiJobClient(final InetSocketAddress remoteAddress) throws IOException {

		this.rpcService = new RPCService(1, CommonTypeUtils.getRPCTypesToRegister());
		this.jobManagementProtocol = this.rpcService.getProxy(remoteAddress, JobManagementProtocol.class);
	}

	public JobSubmissionResult submitJob(final JobGraph jobGraph) throws IOException, InterruptedException {

		return this.jobManagementProtocol.submitJob(jobGraph);
	}

	public void shutdown() {

		this.rpcService.shutDown();
	}
}
