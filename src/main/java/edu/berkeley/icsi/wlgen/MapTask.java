package edu.berkeley.icsi.wlgen;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class MapTask extends MapStub {

	private final PactString key = new PactString();

	private final PactString value = new PactString();

	private final PactRecord outputRecord = new PactRecord();

	@Override
	public void open(final Configuration parameters) {
		System.out.println("OPEN: " + parameters.getString("pact.out.comparator.0", "null"));
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

		out.collect(this.outputRecord);
	}
}
