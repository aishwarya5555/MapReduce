package com.edureka.mapred.assigment2.q23;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class FindAndDeleteEmptyFiles {
	public static void main(String[] args) throws IOException {
		System.out.println("Enter HDFS empty files source and HDFS destination paths...");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String source = br.readLine();
		String dest = br.readLine();

		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://nameservice1");

		listAndDeleteEmptyFiles(source, dest, configuration);
	}

	private static void listAndDeleteEmptyFiles(String source, String dest, Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		Path sourcePath = new Path(source);
		if (!fileSystem.exists(sourcePath)) {
			System.out.println("sourcePath doesn't exists" + source);
			return;
		}
		Path targetPath = new Path(dest);
		if (fileSystem.exists(targetPath)) {
			System.out.println("targetPath already exists" + targetPath);
			fileSystem.delete(targetPath, true);
			System.out.println("targetPath deleted" + targetPath);
		}
		copyFiles(fileSystem, sourcePath, targetPath);
		List<Path> emptyFiles = listEmptyFiles(fileSystem, targetPath);
		// Print empty files
		System.out.println("Empty Files as below::");
		emptyFiles.stream().forEach(p -> System.out.println(p.toString()));
		deleteEmptyFiles(fileSystem, emptyFiles);
		fileSystem.close();
	}

	private static void copyFiles(FileSystem fileSystem, Path sourcePath, Path targetPath)
			throws FileNotFoundException, IOException {
		// read all files from source and copy to destination
		RemoteIterator<LocatedFileStatus> sourceListFiles = fileSystem.listFiles(sourcePath, true);
		while (sourceListFiles.hasNext()) {
			Path sourceFilePath = sourceListFiles.next().getPath();
			String filelocation = sourceFilePath.toString();
			String fileName = filelocation.substring(filelocation.lastIndexOf("/") + 1, filelocation.length());
			Path targetFilePath = new Path(targetPath, fileName);
			writeFile(fileSystem, sourceFilePath, targetFilePath);
		}
	}

	// Print all files in destination
	private static List<Path> listEmptyFiles(FileSystem fileSystem, Path targetPath)
			throws FileNotFoundException, IOException {
		RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(targetPath, true);
		List<Path> emptyFiles = new ArrayList<Path>();
		while (listFiles.hasNext()) {
			LocatedFileStatus sourceFile = listFiles.next();
			String filelocation = sourceFile.getPath().toString();
			String fileName = filelocation.substring(filelocation.lastIndexOf("/") + 1, filelocation.length());
			long fileSize = sourceFile.getLen();
			System.out.println("fileName:" + fileName + " fileSize:" + fileSize);
			if (fileSize == 0) {
				emptyFiles.add(sourceFile.getPath());
			}
		}
		return emptyFiles;
	}

	// delete empty files
	private static void deleteEmptyFiles(FileSystem fileSystem, List<Path> emptyFiles) {
		emptyFiles.stream().forEach(file -> {
			try {
				fileSystem.delete(file, true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
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
