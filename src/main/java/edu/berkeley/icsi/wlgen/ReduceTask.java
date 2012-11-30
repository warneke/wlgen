package edu.berkeley.icsi.wlgen;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;

public class ReduceTask extends ReduceStub {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reduce(final Iterator<PactRecord> records, final Collector<PactRecord> out) throws Exception {

		while (records.hasNext()) {
			out.collect(records.next());
		}

	}

}
