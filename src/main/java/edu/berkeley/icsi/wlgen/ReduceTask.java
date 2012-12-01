package edu.berkeley.icsi.wlgen;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;

public class ReduceTask extends ReduceStub {

	private static final Log LOG = LogFactory.getLog(ReduceTask.class);

	static final String INPUT_OUTPUT_RATIO = "reduce.io.ratio";

	private IORatioAdapter ioRatioAdapter;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void open(final Configuration parameters) {

		final float ioRatio = parameters.getFloat(INPUT_OUTPUT_RATIO, -1.0f);
		if (ioRatio < 0.0f) {
			throw new IllegalStateException("I/O ratio is not set");
		}
		this.ioRatioAdapter = new IORatioAdapter(ioRatio);
		LOG.info("Reduce task initiated with I/O ratio " + ioRatio);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reduce(final Iterator<PactRecord> records, final Collector<PactRecord> out) throws Exception {

		while (records.hasNext()) {
			this.ioRatioAdapter.collect(records.next(), out);
		}

	}

}
