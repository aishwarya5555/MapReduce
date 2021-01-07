package com.edureka.mapred.assigment3.frequency;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequenceMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	public static final Log log = LogFactory.getLog(FrequenceMapper.class);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();
		if (valueStr.length() > 0) {
			String values[] = valueStr.split(" ");
			for (String val : values) {
				String alphaNumericStr = val.replaceAll("[^a-zA-Z0-9]", "");
				context.write(new Text(alphaNumericStr.toLowerCase()), new LongWritable(1));
			}
		} else {
			System.out.println("value:" + valueStr);
		}
	}

}
