package com.edureka.mapred.test.three;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockWithVolumeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long agg = 0;
		for (LongWritable val : values) {
			agg = agg + val.get();
		}
		context.write(new Text(key), new LongWritable(agg));
	}

}
