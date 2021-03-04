package com.edureka.mapred.test.two;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockWithHighPriceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	public static final Log log = LogFactory.getLog(StockWithHighPriceMapper.class);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();
		if (valueStr.length() > 0) {
			String values[] = valueStr.split(",");
			context.write(new Text(values[1]), new DoubleWritable(Double.parseDouble(values[4])));
		}
	}

}
