package com.edureka.mapred.test.two;

import static java.util.Arrays.asList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StockWithHighPrice extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int runStatus = ToolRunner.run(new Configuration(), new StockWithHighPrice(), args);
		System.exit(runStatus);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://nameservice1");
		FileSystem fileSystem = FileSystem.get(conf);

		Job job = Job.getInstance(conf, "StockWithHighPrice");
		job.setJobName("StockWithHighPrice");
		job.setJarByClass(getClass());
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapperClass(StockWithHighPriceMapper.class);
		job.setReducerClass(StockWithHighPriceReducer.class);
		job.setNumReduceTasks(1);

		FileStatus[] sourceListFiles = fileSystem
				.listStatus(new Path("hdfs:///bigdatapgp/common_folder/ect2/NYSE.csv"));

		for (FileStatus fileStatus : asList(sourceListFiles)) {
			Path sourceFilePath = fileStatus.getPath();
			FileInputFormat.addInputPath(job, sourceFilePath);
		}
		Path targetPath = new Path(args[0]);
		if (fileSystem.exists(targetPath)) {
			fileSystem.delete(targetPath, true);
		}
		FileOutputFormat.setOutputPath(job, targetPath);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
