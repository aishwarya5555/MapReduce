package com.edureka.mapred.assigment3.invertedindex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuffer listOfDocuments =new StringBuffer();
		for (Text fileName : values) {
			listOfDocuments.append(fileName).append(" ");
		}
		context.write(new Text(key), new Text(listOfDocuments.toString()));
	}

}
