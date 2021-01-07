package com.edureka.mapred.assigment3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmployeeReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		Double totalSal = 0.0;
		for (Text t : values) {
			String[] split = t.toString().split(",");
			count++;
			totalSal = totalSal + Double.parseDouble(split[1]);
		}
		context.write(new Text("LOCATION=" + key), new Text(" COUNT(EMPNO)=" + count + " TOTAL SAL=" + totalSal));
	}

}
