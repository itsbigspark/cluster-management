package com.bigspark.cloudera.management.helpers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.UUID;

import com.bigspark.cloudera.management.common.model.SourceDescriptor;
import org.slf4j.Logger;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

public class FileSystemHelper {

	static FileSystem fs;
	static Logger logger = LoggerFactory.getLogger(FileSystemHelper.class);

	public static FileSystem getConnection() throws IOException{
		if (fs == null)
			fs = FileSystem.newInstance(SparkHelper.getSparkSession().sparkContext().hadoopConfiguration());
		return fs;
	}

	public static String getFileContent(String path) throws IllegalArgumentException, IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
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

	public static void writeFileContent(String targetPath, String fileName, String payload, Boolean deleteFile) throws IllegalArgumentException, IOException {
		String fullPath = String.format("%s/%s", targetPath, fileName);

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

	public static void copyToLocal(String srcPath, String tgtPath) throws IOException {
		fs.copyToLocalFile(new Path(srcPath), new Path(tgtPath));
	}

	public static void copyFromLocal(String srcPath, String tgtPath) throws IOException {
			fs.copyFromLocalFile(new Path(srcPath), new Path(tgtPath));
	}

	public static String getUserHomeArea() throws IOException {
		return Path.getPathWithoutSchemeAndAuthority(fs.getHomeDirectory()).toString();
	}

	private static FSDataOutputStream getFile(FileSystem fs, String fullPath) throws IllegalArgumentException, IOException {
		if (!fs.exists(new Path(fullPath))) {
			return fs.create(new Path(fullPath));
		} else {
			return fs.append(new Path(fullPath));
		}
	}

	public static void delete(String path, Boolean recursive) throws IOException {
		if(fs.exists(new Path(path))) {
			fs.delete(new Path(path), recursive);
		}
	}

	public static void move(String srcPath, String dstPath) throws IllegalArgumentException, IOException {
		fs.rename(new Path(srcPath), new Path(dstPath));
	}
	
	public static ArrayList<String> list(String path) throws IllegalArgumentException, IOException {
		ArrayList<String> listing = new ArrayList<String>();
		for(FileStatus currFile : fs.listStatus(new Path(path))) {
			listing.add(Path.getPathWithoutSchemeAndAuthority(currFile.getPath()).toString());
		}
		return listing;
	}

	public static String getCreateTrashBaseLocation(String jobType) throws IOException {
		FileSystem fileSystem = FileSystemHelper.getConnection();
		StringBuilder sb = new StringBuilder();
		String userHomeArea = FileSystemHelper.getUserHomeArea();
		long seconds = System.currentTimeMillis() / 1000l;
		sb.append(userHomeArea).append("/.ClusterManagementTrash/"+jobType+"/"+seconds);
		if (! fileSystem.exists(new Path(sb.toString()))){
			fileSystem.mkdirs(new Path(sb.toString()));
		}
		return sb.toString();
	}

	public static Boolean moveDataToUserTrashLocation(String sourceLocation, String trashBaseLocation, Boolean isDryRun, FileSystem fileSystem) throws URISyntaxException, IOException {
		String trashTarget = trashBaseLocation+sourceLocation;
		URI trashTargetURI = new URI(trashTarget);
		String trashTargetParent =  trashTargetURI.getPath().endsWith("/") ? trashTargetURI.resolve("..").toString() : trashTargetURI.resolve(".").toString();
		if (! fileSystem.exists(new Path(trashTargetParent))){
			fileSystem.mkdirs(new Path(trashTargetParent));
		}
		logger.debug("Trash location : "+trashTarget);
		if (!isDryRun){
			try {
				logger.debug("Dropped location :"+sourceLocation+" to Trash");
				boolean isRenameSuccess = fileSystem.rename(new Path(sourceLocation), new Path(trashTarget));
				if (!isRenameSuccess)
					throw new IOException(String.format("Failed to move files from : %s ==> %s", sourceLocation, trashTarget));
				return true;
			} catch (Exception e){
				throw e;
			}
		} else {
			logger.info("DRY RUN - Dropped location :"+sourceLocation+" to Trash");
			return true;
		}
	}
}
