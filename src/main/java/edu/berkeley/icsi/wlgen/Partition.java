package edu.berkeley.icsi.wlgen;

final class Partition {

	static void createPartition(final int numberOfReducers, final long sizeOfIntermediateInput, final long minimum,
			final long percentile10, final long percentile25, final long median, final long percentile75,
			final long percentile90, final long maximum) {

		final long partition[] = new long[numberOfReducers];

		for (int i = 0; i < numberOfReducers; ++i) {
			partition[i] = -1L;
		}

		partition[0] = minimum;
		partition[toIndex(numberOfReducers, 10)] = percentile10;
		partition[toIndex(numberOfReducers, 25)] = percentile25;
		partition[toIndex(numberOfReducers, 50)] = median;
		partition[toIndex(numberOfReducers, 75)] = percentile75;
		partition[toIndex(numberOfReducers, 90)] = percentile90;
		partition[numberOfReducers - 1] = maximum;

		/*
		 * System.out.println("Total:\t" + mrj.getSizeOfIntermediateData());
		 * System.out.println("Maximum:\t" + maximum);
		 * System.out.println("Median:\t" + median);
		 * System.out.println("75 percentile:\t" + percentile75);
		 * System.out.println("Minimum:\t" + minimum);
		 */
		if (numberOfReducers == 1) {
			partition[0] = sizeOfIntermediateInput;
		} else {
			printPartition(partition);
		}
	}

	private static int toIndex(final int arrayLength, final int percentile) {

		final double unit = arrayLength / 100.0;

		return (int) Math.floor(unit * percentile);
	}

	static void printPartition(final long[] partition) {

		System.out.println("====================================================================================");
		for (int i = 0; i < partition.length; ++i) {
			if (partition[i] != -1L) {
				System.out.println(i + "\t" + partition[i]);
			}
			//if (i < (partition.length - 1)) {
			//	System.out.print(":");
			//}

		}
		//System.out.println("");
	}
}
