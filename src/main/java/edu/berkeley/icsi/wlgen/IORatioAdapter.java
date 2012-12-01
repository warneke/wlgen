package edu.berkeley.icsi.wlgen;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;

final class IORatioAdapter {

	private final boolean reduceMode;

	private final int ratio;

	private int counter = 0;

	IORatioAdapter(final float ioRatio) {

		if (ioRatio >= 1.0f) {
			this.reduceMode = true;
			this.ratio = Math.round(ioRatio);
		} else {
			this.reduceMode = false;
			this.ratio = Math.round(1.0f / ioRatio);
		}
	}

	void collect(final PactRecord record, final Collector<PactRecord> out) {

		if (this.reduceMode) {
			if (++this.counter == this.ratio) {
				out.collect(record);
				this.counter = 0;
			}

		} else {
			System.out.println("NON-REDUCE MODE");
		}

	}
}
