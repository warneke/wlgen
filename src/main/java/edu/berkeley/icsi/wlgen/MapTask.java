package edu.berkeley.icsi.wlgen;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class MapTask extends MapStub {

	private final PactString key = new PactString();

	private final PactString value = new PactString();

	private final PactRecord outputRecord = new PactRecord();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void map(final PactRecord record, final Collector<PactRecord> out) throws Exception {

		final PactString line = record.getField(0, PactString.class);

		line.substring(key, 0, 10);
		line.substring(value, 11);

		this.outputRecord.setField(0, key);
		this.outputRecord.setField(1, value);

		out.collect(this.outputRecord);
	}
}
