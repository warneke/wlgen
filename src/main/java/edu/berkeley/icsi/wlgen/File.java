package edu.berkeley.icsi.wlgen;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

final class File {

	private final Set<MapReduceJob> usedAsInput = new HashSet<MapReduceJob>();

	private final Set<MapReduceJob> usedAsOutput = new HashSet<MapReduceJob>();

	private final long size;

	File(final long size) {
		this.size = size;
	}

	void usedAsInputBy(final MapReduceJob mapReduceJob) {

		this.usedAsInput.add(mapReduceJob);
	}

	void usedAsOutputBy(final MapReduceJob mapReduceJob) {

		this.usedAsOutput.add(mapReduceJob);
	}

	Iterator<MapReduceJob> inputIterator() {

		return this.usedAsInput.iterator();
	}

	Iterator<MapReduceJob> outputIterator() {

		return this.usedAsOutput.iterator();
	}

	int getNumberOfInputUsages() {

		return this.usedAsInput.size();
	}

	int getNumberOfOutputUsages() {

		return this.usedAsOutput.size();
	}
}
