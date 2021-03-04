package com.edureka.mapred.test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordOfOccurrenceReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long occurrence = 0;
		for (LongWritable val : values) {
			occurrence = occurrence + val.get();
		}
		if (occurrence < 100) {
			context.write(new Text(key), new LongWritable(occurrence));
		}
	}

}
