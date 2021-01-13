package com.edureka.mapred.assigment2.q25;

import static java.lang.Double.parseDouble;
import static org.apache.commons.lang.StringUtils.isEmpty;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WeatherReportApp {

	public static void main(String[] args) throws Exception {
		System.out.println("Enter HDFS destination paths...");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String source = "hdfs://nameservice1/bigdatapgp/common_folder/assignment2/weather_dataset/weather1.csv";
		String dest = br.readLine();

		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://nameservice1");
		FileSystem fileSystem = FileSystem.get(configuration);

		readWriteWeatherReport(source, dest, fileSystem);
		fileSystem.close();
	}

	private static void readWriteWeatherReport(String source, String dest, FileSystem fileSystem)
			throws IOException, ParseException {
		Path sourcePath = new Path(source);
		if (fileSystem.exists(sourcePath)) {
			FSDataInputStream in = fileSystem.open(sourcePath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String header = br.readLine() + "\n", line;
			String indiaReport = "india_weather.csv";
			String othersReport = "row_weather.csv";
			Path indiaPath = new Path(dest + indiaReport);
			Path othersPath = new Path(dest + othersReport);
			deleteIfExist(fileSystem, indiaPath);
			deleteIfExist(fileSystem, othersPath);
			FSDataOutputStream indiaOut = fileSystem.create(indiaPath);
			FSDataOutputStream otherOut = fileSystem.create(othersPath);
			BufferedWriter indiabw = new BufferedWriter(new OutputStreamWriter(indiaOut));
			indiabw.write(header);
			BufferedWriter otherbw = new BufferedWriter(new OutputStreamWriter(otherOut));
			otherbw.write(header);

			while ((line = br.readLine()) != null) {
				String[] record = line.split(",");
				WeatherReport weatherRecord = weatherReport(record);
				if ("INDIA".equalsIgnoreCase(weatherRecord.getCountry())) {
					indiabw.write(weatherRecord.toString());
				} else {
					otherbw.write(weatherRecord.toString());
				}
			}
			indiaOut.close();
			otherOut.close();
		} else {
			System.out.println("source path doesnt exists" + source);
		}
	}

	private static void deleteIfExist(FileSystem fileSystem, Path path) throws IOException {
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}
	}

	private static WeatherReport weatherReport(String[] record) throws ParseException {
		return new WeatherReport(record[0], isEmpty(record[1]) ? 0.0 : parseDouble(record[1]),
				isEmpty(record[2]) ? 0.0 : parseDouble(record[2]), record[3], record[4], record[5], record[6]);
	}

}
