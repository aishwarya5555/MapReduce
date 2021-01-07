package com.edureka.mapred.assigment3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmployeeMapper extends Mapper<LongWritable, Text, Text, Text> {
	public static final Log log = LogFactory.getLog(EmployeeMapper.class);

	private static Map<String, String> deptMap = new HashMap<String, String>();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		URI[] cacheFiles = context.getCacheFiles();
		if (cacheFiles != null && cacheFiles.length > 0) {
			loadDepartmentsHashMap(cacheFiles[0].getPath(), context);
		} else {
			System.out.println("no cacheFiles");
		}
	}

	private void loadDepartmentsHashMap(String filePath, Context context) throws IOException {
		try {
			String strLineRead = "";
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path getFilePath = new Path(filePath);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
			String headerLine = reader.readLine();
			while ((strLineRead = reader.readLine()) != null) {
				System.out.println("strLineRead" + strLineRead);
				String deptFieldArray[] = strLineRead.split(",");
				deptMap.put(deptFieldArray[0].trim(), deptFieldArray[2].trim());
			}
		} catch (Exception e) {
			System.out.println("Unable to read the File");
			System.exit(1);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		if (key.get() != 0) {
			if (value.toString().length() > 0) {
				String arrEmpAttributes[] = value.toString().split(",");
				String location = deptMap.get(arrEmpAttributes[7]);
				if (location == null) {
					location = "UNKNOWN";
				}
				context.write(new Text(location), new Text(arrEmpAttributes[0] + "," + arrEmpAttributes[5]));
			} else {
				System.out.println("value:" + value.toString());
			}
		}

		System.out.println("value:" + value.toString());

	}

}
