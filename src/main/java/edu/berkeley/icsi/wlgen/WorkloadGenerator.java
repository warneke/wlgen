package edu.berkeley.icsi.wlgen;

import java.io.IOException;

public final class WorkloadGenerator {

	private void loadWorkloadTraces() throws IOException {

		final MapReduceWorkload mrw = MapReduceWorkload.reconstructFromTraces();

	}

	public static void main(final String[] args) throws IOException {

		final WorkloadGenerator wlg = new WorkloadGenerator();
		wlg.loadWorkloadTraces();
	}

}
