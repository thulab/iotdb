package cn.edu.thu.tsfiledb.auth2.manage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

import cn.edu.thu.tsfiledb.auth2.exception.NoSuchUserException;
import cn.edu.thu.tsfiledb.auth2.model.User;
import cn.edu.thu.tsfiledb.auth2.model.Usermeta;

public class UserManager {
	private static String userFolder = AuthConfig.userFolder;
	private static String userInfoFile = "userInfo";
	private static UserManager instance;
	
	private HashMap<String, User> users = new HashMap<>();
	
	private UserManager() {
		
	}
	
	public static UserManager getInstance() throws IOException {
		if(instance == null) {
			instance = new UserManager();
			instance.init();
		}
		return instance;
	}
	
	private void init() throws IOException {
		File userFolderFile = new File(userFolder);
		File infoFile = new File(userFolder + userInfoFile);
		if(!userFolderFile.exists())
			userFolderFile.mkdirs();
		if(!infoFile.exists())
			infoFile.createNewFile();
		RandomAccessFile raf = new RandomAccessFile(infoFile, "r");
		while(raf.getFilePointer() + User.RECORD_SIZE < raf.length()) {
			User user = User.readObject(raf);
			if(!user.getUsername().equals(""))
				users.put(user.getUsername(), user);
		}
		raf.close();
		
		if(findUser("root") == null) {
			createUser("root", "root");
		}
	}
	
	public User findUser(String username) {
		return users.get(username);
	}
	
	synchronized public boolean createUser(String username, String password) throws IOException {
		if(users.get(username) != null) {
			return false;
		}
		Usermeta usermeta = Usermeta.getInstance();
		int newUserID = usermeta.getMaxUID();
		User newUser = new User(username, password, newUserID);
		RandomAccessFile raf = new RandomAccessFile(userFolder + userInfoFile, "rw");
		raf.seek(raf.length());
		newUser.writeObject(raf);
		usermeta.increaseMaxRID();
		users.put(username, newUser);
		
		NodeManager.getInstance().initForUser(newUserID);
		
		raf.close();
		return true;
	}
	
	public boolean authorize(String username, String password) {
		User user = users.get(username);
		if(user == null) {
			return false;
		}
		return user.getPassword().equals(password);
	}
	
	public boolean deleteUser(String username) throws IOException {
		User user = users.get(username);
		if(user == null) {
			return false;
		}
		users.remove(username);
		User blankUser = new User("", "", user.getID());
		flushUser(blankUser);
		NodeManager.getInstance().cleanForUser(user.getID());
		return true;
	}
	
	private void flushUser(User user) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(userFolder + userInfoFile, "rw");
		raf.seek(user.getID() * User.RECORD_SIZE);
		user.writeObject(raf);
		raf.close();
	}
	
	public boolean modifyPW(String username, String newPassword) throws IOException, NoSuchUserException {
		User user = users.get(username);
		if(user == null) {
			throw new NoSuchUserException(username + " does not exist");
		}
		user.setPassword(newPassword);
		flushUser(user);
		return true;
	}
	
	public User[] getAllUsers() {
		return users.values().toArray(new User[0]);
	}
}
