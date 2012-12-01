package edu.berkeley.icsi.wlgen;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class MapTask extends MapStub {

	private static final Log LOG = LogFactory.getLog(MapTask.class);

	static final String INPUT_OUTPUT_RATIO = "map.io.ratio";

	private final PactString key = new PactString();

	private final PactString value = new PactString();

	private final PactRecord outputRecord = new PactRecord();

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
		LOG.info("Map task initiated with I/O ratio " + ioRatio);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void map(final PactRecord record, final Collector<PactRecord> out) throws Exception {

		final PactString line = record.getField(0, PactString.class);

		line.substring(this.key, 0, 8);
		line.substring(this.value, 9);

		this.outputRecord.setField(0, this.key);
		this.outputRecord.setField(1, this.value);

		this.ioRatioAdapter.collect(this.outputRecord, out);
	}
}
