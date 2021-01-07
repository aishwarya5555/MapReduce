package com.edureka.mapred.assigment3.weatherdataset;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WeatherReportPartitioner extends Partitioner<IntWritable, Text> {

	enum CENTURY_RANGE {
		EIGHTEENTH(1700, 1799, 0), NINETEENTH(1800, 1899, 1), TWENTIETH(1900, 1999, 2), TWENTY_FIRST(2000, 2099, 3);
		private int minYear;
		private int maxYear;
		private int partitionIndex;

		CENTURY_RANGE(int minYear, int maxYear, int centuryIndex) {
			this.minYear = minYear;
			this.maxYear = maxYear;
			this.partitionIndex = centuryIndex;
		}

		static int getCentury(int year) {
			for (CENTURY_RANGE century : values()) {
				if (year >= century.minYear && year <= century.maxYear) {
					return century.partitionIndex;
				}
			}
			return TWENTY_FIRST.partitionIndex;
		}

		public int getMaxYear() {
			return maxYear;
		}

	}

	@Override
	public int getPartition(IntWritable key, Text value, int numPartitions) {
		return CENTURY_RANGE.getCentury(key.get());
	}

}
