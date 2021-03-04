package com.edureka.mapred.test;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordOfOccurrenceMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	public static final Log log = LogFactory.getLog(WordOfOccurrenceMapper.class);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();
		if (valueStr.length() > 0) {
			String values[] = valueStr.split(" ");
			for (String val : values) {
				String alphaNumericStr = val.replaceAll("[^a-zA-Z]", "");
				context.write(new Text(alphaNumericStr.toLowerCase()), new LongWritable(1));
			}
		}
	}

}
