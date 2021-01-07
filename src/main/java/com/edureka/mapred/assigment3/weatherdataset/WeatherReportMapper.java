package com.edureka.mapred.assigment3.weatherdataset;

import static java.lang.Integer.parseInt;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherReportMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	public static final Log log = LogFactory.getLog(WeatherReportMapper.class);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		if (value.toString().length() > 0) {
			String dataset[] = value.toString().split(",");
			if (dataset[0].equalsIgnoreCase("dt")) {
				System.out.println("Ignore Header line" + value.toString());
			} else {
				int year = parseInt(dataset[0].substring(0, 4));
				String country = dataset[4];
				if ("india".equalsIgnoreCase(country)) {
					context.write(new IntWritable(year), value);
				}
			}
		} else {
			System.out.println("value:" + value.toString());
		}
	}

}
