package cn.edu.tsinghua.postback.iotdb.sender;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.postback.conf.PostBackConfig;
import cn.edu.tsinghua.iotdb.postback.conf.PostBackDescriptor;

public class FileManager {
	
	private Set<String> sendingFiles;
	private Set<String> lastLocalFiles;
	private Set<String> nowLocalFiles;
	private PostBackConfig config= PostBackDescriptor.getInstance().getConfig();

	private static final Logger LOGGER = LoggerFactory.getLogger(FileManager.class);
	private static class FileManagerHolder{
		private static final FileManager INSTANCE = new FileManager();
	}
	private FileManager() {}
	

	public static final FileManager getInstance() {
		return FileManagerHolder.INSTANCE;
	}

	public void getSendingFileList() {
		Set<String> oldFiles = lastLocalFiles;
		Set<String> newFiles = nowLocalFiles;
		Set<String> fileList = new HashSet<>();
		for(String newFile : newFiles) {
			if(!oldFiles.contains(newFile)) {
				fileList.add(newFile);
			}
		}
		sendingFiles = fileList;
		LOGGER.info("IoTDB sender : Sender get the sendingFils list!");
		for(String filePath: sendingFiles)
        {
        	System.out.println(filePath);
        }		
	}

	public void getLastLocalFileList(String path)  {
		Set<String> fileList = new HashSet<>();
		File file = new File(path);
		try {
			if (!file.exists()) {
				file.createNewFile();
			} else {
				BufferedReader bf = null;
				try {
					bf = new BufferedReader(new FileReader(file));
					String fileName = null;
					while ((fileName = bf.readLine()) != null) {
						fileList.add(fileName);
					}
					bf.close();
				} catch (IOException e) {
					LOGGER.error("IoTDB post back sender: cannot get last pass local file list when reading file {} because {}", config.LAST_FILE_INFO, e.getMessage());
				} finally {
					if(bf != null) {
						bf.close();
					}
				}
			}
		} catch (IOException e) {
			LOGGER.error("IoTDB post back sender: cannot get last pass local file list because {}", e.getMessage());
		}
		lastLocalFiles = fileList;
	}

	public void getNowLocalFileList(String path) {
		
		Set<String> fileList = new HashSet<>();
		try (Stream<Path> filePathStream = Files.walk(Paths.get(path))) {
			filePathStream.filter(Files::isRegularFile).forEach(filePath -> {
				//if it is root.stats, do not postBack it.
				String absolutePath = filePath.toFile().getAbsolutePath();
				if(!absolutePath.contains("root.stats")){
					fileList.add(absolutePath);
				}
			});
		} catch (IOException e) {
			LOGGER.error("IoTDB post back sender: cannot get now local file list because {}", e.getMessage());
		}
		Iterator<String> it = fileList.iterator();
		List<String> tempFilePath = new ArrayList<>();
		tempFilePath.clear();
		while (it.hasNext()) {  
		    String str = it.next();  
		    if (str.endsWith(".restore")) {
		        it.remove();
		        tempFilePath.add(str.replaceAll(".restore", ""));
		    }  
		}  
		for(String absolutePath:tempFilePath) {
			if(fileList.contains(absolutePath)) {
					fileList.remove(absolutePath);
			}
		}
		nowLocalFiles = fileList;		
	}

	public void backupNowLocalFileInfo(String backupFile) {
		BufferedWriter bufferedWriter = null;
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(backupFile));
			for(String file : nowLocalFiles) {
				bufferedWriter.write(file+"\n");
			}
		} catch (IOException e) {
			LOGGER.error("IoTDB post back sender: cannot back up now local file info because {}", e);
		} finally {
			if(bufferedWriter != null) {
				try {
					bufferedWriter.close();
				} catch (IOException e) {
					LOGGER.error("IoTDB post back sender: cannot close stream after backing up now local file info because {}", e);
				}
			}
		}
	}

	public Set<String> getSendingFiles() {
		return sendingFiles;
	}

	public Set<String> getLastLocalFiles() {
		return lastLocalFiles;
	}

	public Set<String> getNowLocalFiles() {
		return nowLocalFiles;
	}
}