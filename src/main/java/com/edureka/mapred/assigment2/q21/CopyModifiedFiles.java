package com.edureka.mapred.assigment2.q21;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class CopyModifiedFiles {
	public static void main(String[] args) throws IOException {
		System.out.println("Enter local source and HDFS destination paths...");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String source = br.readLine();
		String dest = br.readLine();

		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://nameservice1");

		overrideModifiedFilesOnly(source, dest, configuration);
	}

	private static void overrideModifiedFilesOnly(String source, String dest, Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		Path sourcePath = new Path(source);
		if (!fileSystem.exists(sourcePath)) {
			System.out.println("sourcePath doesn't exists" + source);
			return;
		}
		Path targetPath = new Path(dest);
		if (!fileSystem.exists(targetPath)) {
			System.out.println("targetPath doesn't exists" + source);
			return;
		}
		RemoteIterator<LocatedFileStatus> sourceListFiles = fileSystem.listFiles(sourcePath, true);
		while (sourceListFiles.hasNext()) {
			Path sourceFilePath = sourceListFiles.next().getPath();
			String filelocation = sourceFilePath.toString();
			String fileName = filelocation.substring(filelocation.lastIndexOf("/") + 1, filelocation.length());
			Path targetFilePath = new Path(targetPath, fileName);
			if (!fileSystem.exists(targetFilePath)) {
				System.out.println("targetFilePath doesn't exists" + targetFilePath);
				writeFile(fileSystem, sourceFilePath, targetFilePath);
			} else {
				FileChecksum sourceFileChecksum = fileSystem.getFileChecksum(sourceFilePath);
				FileChecksum targetFileChecksum = fileSystem.getFileChecksum(targetFilePath);
				if (sourceFileChecksum.equals(targetFileChecksum)) {
					System.out.println("File is not modified:" + fileName);
				} else {
					System.out.println("File is modified:" + fileName);
					System.out.println("src fileName:" + sourceFilePath + " sourceFileChecksum:" + sourceFileChecksum);
					System.out
							.println("target fileName:" + targetFilePath + " targetFileChecksum:" + targetFileChecksum);
					writeFile(fileSystem, sourceFilePath, targetFilePath);
				}
			}
			fileSystem.close();
		}
	}

	private static void writeFile(FileSystem fileSystem, Path sourcePath, Path targetFilePath) throws IOException {
		FSDataInputStream in = fileSystem.open(sourcePath);
		FSDataOutputStream out = fileSystem.create(targetFilePath);
		byte[] bf = new byte[1024];
		int noOfbytes = 0;
		while ((noOfbytes = in.read(bf)) > -1) {
			out.write(bf, 0, noOfbytes);
		}
		in.close();
		out.close();
	}
}
