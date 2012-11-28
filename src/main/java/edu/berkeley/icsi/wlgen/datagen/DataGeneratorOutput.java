package edu.berkeley.icsi.wlgen.datagen;

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;
import eu.stratosphere.nephele.types.StringRecord;

public class DataGeneratorOutput extends AbstractFileOutputTask {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {

		new RecordReader<StringRecord>(this, StringRecord.class);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {
	}

}
