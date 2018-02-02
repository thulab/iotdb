package cn.edu.tsinghua.postback.iotdb.sender;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.postback.iotdb.sender.FileManager;

public class FileManagerTest {

	public static final String POST_BACK_DIRECTORY_TEST = "postback" + File.separator;
	public static final String LAST_FILE_INFO_TEST = POST_BACK_DIRECTORY_TEST + "lastLocalFileList.txt";
	public static final String SENDER_FILE_PATH_TEST = POST_BACK_DIRECTORY_TEST + "data";
	FileManager manager = FileManager.getInstance();

	@Before
	public void setUp() throws Exception {
		File file =new File(LAST_FILE_INFO_TEST);
		if (!file.getParentFile().exists()) {
			file.getParentFile().mkdirs();
		}
		if (!file.exists()) {
			file.createNewFile();
		}
		file =new File(SENDER_FILE_PATH_TEST);
		if (!file.exists()) {
			file.mkdirs();
		}
	}

	@After
	public void tearDown() throws Exception {
		delete(new File(POST_BACK_DIRECTORY_TEST));
		new File(POST_BACK_DIRECTORY_TEST).delete();
	}
	
	public void delete(File file) {
		if (file.isFile() || file.list().length == 0) {
			file.delete();
		} 
		else{
			File[] files = file.listFiles();
			for (File f : files) {
				delete(f);        
				f.delete();       
			}
		}		
	}

	@Test //It tests two classes : backupNowLocalFileInfo and getLastLocalFileList
	public void testBackupNowLocalFileInfo() throws IOException {
		Set<String> allFileList = new HashSet<>();
		
		// TODO create some files
		Random r = new Random(0);
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 5; j++) {
				String rand = String.valueOf(r.nextInt(10000));
				String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
				File file = new File(fileName);
				allFileList.add(file.getAbsolutePath());
				if (!file.getParentFile().exists()) {
					file.getParentFile().mkdirs();
				}
				if (!file.exists()) {
					file.createNewFile();
				}
			}
		}
		Set<String> lastFileList = new HashSet<>();
		
		//lastFileList is empty
		manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
		lastFileList = manager.getLastLocalFiles();
		assert (lastFileList.isEmpty());
		
		//add some files
 		manager.getNowLocalFileList(SENDER_FILE_PATH_TEST);
		manager.backupNowLocalFileInfo(LAST_FILE_INFO_TEST);
		manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
		lastFileList = manager.getLastLocalFiles();
		assert (lastFileList.size() == allFileList.size() && lastFileList.containsAll(allFileList));
		
		//add some files and delete some files
		r = new Random(1);
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 5; j++) {
				String rand = String.valueOf(r.nextInt(10000));
				String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator +rand;
				File file = new File(fileName);
				allFileList.add(file.getAbsolutePath());
				if (!file.getParentFile().exists()) {
					file.getParentFile().mkdirs();
				}
				if (!file.exists()) {
					file.createNewFile();
				}
			}
		}
		int count = 0;
		Set<String> deleteFile = new HashSet<>();
		for(String path:allFileList){
			count++;
			if(count % 3 == 0){
				deleteFile.add(path);
			}
		}
		for(String path:deleteFile){
			new File(path).delete();
			allFileList.remove(path);				
		}
 		manager.getNowLocalFileList(SENDER_FILE_PATH_TEST);
		manager.backupNowLocalFileInfo(LAST_FILE_INFO_TEST);
		manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
		lastFileList = manager.getLastLocalFiles();
//		assert (lastFileList.size() == allFileList.size()) && lastFileList.containsAll(allFileList);
	}

	@Test
	public void testGetNowLocalFileList() throws IOException {
		Set<String> allFileList = new HashSet<>();
 		Set<String> fileList = new HashSet<>();
 		
 		//nowLocalList is empty
 		manager.getNowLocalFileList(SENDER_FILE_PATH_TEST);
		fileList = manager.getNowLocalFiles();
		assert (fileList.isEmpty());
		
		//add some files
		Random r = new Random(0);
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 5; j++) {
				String rand = String.valueOf(r.nextInt(10000));
				String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
				File file = new File(fileName);
				allFileList.add(file.getAbsolutePath());
				if (!file.getParentFile().exists()) {
					file.getParentFile().mkdirs();
				}
				if (!file.exists()) {
					file.createNewFile();
				}
			}
		}
		manager.getNowLocalFileList(SENDER_FILE_PATH_TEST);
		fileList = manager.getNowLocalFiles();
		assert (allFileList.size() == fileList.size() && fileList.containsAll(allFileList));
		
		//delete some files and add some files 
		int count = 0;
		Set<String> deleteFile = new HashSet<>();
		for(String path:allFileList){
			count++;
			if(count % 3 == 0){
				deleteFile.add(path);
			}
		}
		for(String path:deleteFile){
			new File(path).delete();
			allFileList.remove(path);				
		}
		r = new Random(1);
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 5; j++) {
				String rand = String.valueOf(r.nextInt(10000));
				String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
				File file = new File(fileName);
				allFileList.add(file.getAbsolutePath());
				if (!file.getParentFile().exists()) {
					file.getParentFile().mkdirs();
				}
				if (!file.exists()) {
					file.createNewFile();
				}
			}
		}
		manager.getNowLocalFileList(SENDER_FILE_PATH_TEST);
		fileList = manager.getNowLocalFiles();
		assert (allFileList.size() == fileList.size()) && fileList.containsAll(allFileList);
	}

	@Test
	public void testGetSendingFileList() throws IOException {
		Set<String> allFileList = new HashSet<>();
 		Set<String> newFileList = new HashSet<>();
 		Set<String> sendingFileList = new HashSet<>();
 		Set<String> lastlocalList = new HashSet<>();
 		
 		//nowSendingList is empty
 		manager.getNowLocalFileList(SENDER_FILE_PATH_TEST);
		allFileList = manager.getNowLocalFiles();
 		manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
 		lastlocalList = manager.getLastLocalFiles();
 		manager.getSendingFileList();
 		sendingFileList = manager.getSendingFiles();
		assert (sendingFileList.isEmpty());
		
		//add some files
		newFileList.clear();
		manager.backupNowLocalFileInfo(LAST_FILE_INFO_TEST);
		Random r = new Random(0);
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 5; j++) {
				String rand = String.valueOf(r.nextInt(10000));
				String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
				File file = new File(fileName);
				allFileList.add(file.getAbsolutePath());
				newFileList.add(file.getAbsolutePath());
				if (!file.getParentFile().exists()) {
					file.getParentFile().mkdirs();
				}
				if (!file.exists()) {
					file.createNewFile();
				}
			}
		}
		manager.getNowLocalFileList(SENDER_FILE_PATH_TEST);
		allFileList = manager.getNowLocalFiles();
 		manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
 		lastlocalList = manager.getLastLocalFiles();
 		manager.getSendingFileList();
 		sendingFileList = manager.getSendingFiles();
		assert (sendingFileList.size() == newFileList.size()) && sendingFileList.containsAll(newFileList);
		
		//delete some files and add some files 
		int count = 0;
		Set<String> deleteFile = new HashSet<>();
		for(String path:allFileList){
			count++;
			if(count % 3 == 0){
				deleteFile.add(path);
			}
		}
		for(String path:deleteFile){
			new File(path).delete();
			allFileList.remove(path);
		}
		newFileList.clear();
		manager.backupNowLocalFileInfo(LAST_FILE_INFO_TEST);
		r = new Random(1);
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 5; j++) {
				String rand = String.valueOf(r.nextInt(10000));
				String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
				File file = new File(fileName);
				allFileList.add(file.getAbsolutePath());
				newFileList.add(file.getAbsolutePath());
				if (!file.getParentFile().exists()) {
					file.getParentFile().mkdirs();
				}
				if (!file.exists()) {
					file.createNewFile();
				}
			}
		}
		manager.getNowLocalFileList(SENDER_FILE_PATH_TEST);
		allFileList = manager.getNowLocalFiles();
 		manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
 		lastlocalList = manager.getLastLocalFiles();
 		manager.getSendingFileList();
 		sendingFileList = manager.getSendingFiles();
		assert (sendingFileList.size() == newFileList.size()) && sendingFileList.containsAll(newFileList);
	}
}