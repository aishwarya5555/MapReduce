package com.edureka.mapred.test.two;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockWithHighPriceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		double maxPrice = 0;
		for (DoubleWritable val : values) {
			if (val.get() > maxPrice) {
				maxPrice = val.get();
			}
		}
		context.write(new Text(key), new DoubleWritable(maxPrice));
	}

}
