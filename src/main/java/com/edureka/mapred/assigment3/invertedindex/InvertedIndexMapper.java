package com.edureka.mapred.assigment3.invertedindex;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	public static final Log log = LogFactory.getLog(InvertedIndexMapper.class);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if (value.toString().length() > 0) {
			String arrEmpAttributes[] = value.toString().split(",");
			String firstName = arrEmpAttributes[0];
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			context.write(new Text(firstName), new Text(fileName));
		} else {
			System.out.println("value:" + value.toString());
		}
	}

}
