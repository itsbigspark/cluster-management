package com.bigspark.cloudera.management.helpers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemHelper {

	static FileSystem fs;

	public static FileSystem getConnection() throws IOException{
		if (fs == null)
			fs = FileSystem.get(SparkHelper.getSparkSession().sparkContext().hadoopConfiguration());
			return fs;
	}

	public static String getFileContent(String path) throws IllegalArgumentException, IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		try(FileSystem fs = getConnection()) {
			if (fs.exists(new Path(path))) {
				FSDataInputStream in = fs.open(new Path(path));
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String line = null;
				while ((line = br.readLine()) != null) {
					sb.append(line + "\r\n");
				}
			} else {
			}
			return sb.toString();
		}
	}

	public static void writeFileContent(String targetPath, String fileName, String payload, Boolean deleteFile) throws IllegalArgumentException, IOException {
		String fullPath = String.format("%s/%s", targetPath, fileName);
		try(FileSystem fs = getConnection()) {
			// Make sure drive exists
			if (!fs.exists(new Path(targetPath))) {
				fs.mkdirs(new Path(targetPath));
			}

			if (deleteFile) {
				fs.delete(new Path(fullPath), false);
			}

			try (FSDataOutputStream fso = getFile(fs, fullPath)) {;
			fso.write(payload.getBytes());
			}
		}
	}

	public static void copyToLocal(String srcPath, String tgtPath) throws IOException {
		try(FileSystem fs = getConnection()) {
			fs.copyToLocalFile(new Path(srcPath), new Path(tgtPath));
		}
	}

	public static void copyFromLocal(String srcPath, String tgtPath) throws IOException {
		try(FileSystem fs = getConnection()) {
			fs.copyFromLocalFile(new Path(srcPath), new Path(tgtPath));
		}
	}

	public static String getUserHomeArea() throws IOException {
		try(FileSystem fs = getConnection()) {
			return Path.getPathWithoutSchemeAndAuthority(fs.getHomeDirectory()).toString();
		}
	}

	private static FSDataOutputStream getFile(FileSystem fs, String fullPath) throws IllegalArgumentException, IOException {
		if (!fs.exists(new Path(fullPath))) {
			return fs.create(new Path(fullPath));
		} else {
			return fs.append(new Path(fullPath));
		}
	}

	public static void delete(String path, Boolean recursive) throws IOException {
		try(FileSystem fs = getConnection()) {
			if(fs.exists(new Path(path))) {
				fs.delete(new Path(path), recursive);
			}
		}
	}

	public static void move(String srcPath, String dstPath) throws IllegalArgumentException, IOException {
		try(FileSystem fs = getConnection()) {
			fs.rename(new Path(srcPath), new Path(dstPath));
		}
	}
	
	public static ArrayList<String> list(String path) throws IllegalArgumentException, IOException {
		ArrayList<String> listing = new ArrayList<String>();
		try(FileSystem fs = getConnection()) {
			for(FileStatus currFile : fs.listStatus(new Path(path))) {
				listing.add(Path.getPathWithoutSchemeAndAuthority(currFile.getPath()).toString());
			}
		}
		return listing;
	}
}
