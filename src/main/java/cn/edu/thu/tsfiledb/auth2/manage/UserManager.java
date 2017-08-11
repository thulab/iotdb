package cn.edu.thu.tsfiledb.auth2.manage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

import cn.edu.thu.tsfiledb.auth2.exception.NoSuchUserException;
import cn.edu.thu.tsfiledb.auth2.model.User;
import cn.edu.thu.tsfiledb.auth2.model.Usermeta;
import cn.edu.thu.tsfiledb.conf.AuthConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;

public class UserManager {
	private static AuthConfig authConfig = TsfileDBDescriptor.getInstance().getConfig().authConfig;
	
	private static String userFolder = authConfig.USER_FOLDER;
	private static String userInfoFile = authConfig.USER_INFO_FILE;

	private HashMap<String, User> users = new HashMap<>();
	
	private boolean initialized = false;
	
	private static class InstanceHolder {
		private static final UserManager instance = new UserManager();
	}

	private UserManager() {

	}

	public static UserManager getInstance() throws IOException {
		if (!InstanceHolder.instance.initialized) {
			InstanceHolder.instance.init();
		}
		return InstanceHolder.instance;
	}

	private void init() throws IOException {
		File userFolderFile = new File(userFolder);
		File infoFile = new File(userFolder + userInfoFile);
		if (!userFolderFile.exists())
			userFolderFile.mkdirs();
		if (!infoFile.exists())
			infoFile.createNewFile();
		RandomAccessFile raf = new RandomAccessFile(infoFile, authConfig.RAF_READ);
		try {
			while (raf.getFilePointer() + User.RECORD_SIZE < raf.length()) {
				User user = User.readObject(raf);
				if (!user.getUserName().equals(""))
					users.put(user.getUserName(), user);
			}
		} finally {
			raf.close();
		}
		
		if (findUser(authConfig.SUPER_USER) == null) {
			createUser(authConfig.SUPER_USER, authConfig.SUPER_USER);
		}
		initialized = true;
	}

	public User findUser(String username) {
		return users.get(username);
	}

	synchronized public boolean createUser(String username, String password) throws IOException {
		if (users.get(username) != null) {
			return false;
		}
		Usermeta usermeta = Usermeta.getInstance();
		int newUserID = usermeta.getMaxUID();
		User newUser = new User(username, password, newUserID);
		RandomAccessFile raf = new RandomAccessFile(userFolder + userInfoFile, authConfig.RAF_READ_WRITE);
		try {
			raf.seek(raf.length());
			newUser.writeObject(raf);
			usermeta.increaseMaxRID();
			users.put(username, newUser);
			NodeManager.getInstance().initForUser(newUserID);
		} finally {
			raf.close();
		}
		return true;
	}

	public boolean authorize(String username, String password) {
		User user = users.get(username);
		if (user == null) {
			return false;
		}
		return user.getPassword().equals(password);
	}

	public boolean deleteUser(String username) throws IOException {
		User user = users.get(username);
		if (user == null) {
			return false;
		}
		users.remove(username);
		User blankUser = new User("", "", user.getID());
		flushUser(blankUser);
		NodeManager.getInstance().cleanForUser(user.getID());
		return true;
	}

	private void flushUser(User user) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(userFolder + userInfoFile, authConfig.RAF_READ_WRITE);
		try {
			raf.seek(user.getID() * User.RECORD_SIZE);
			user.writeObject(raf);
		} finally {
			raf.close();
		}
	}

	public boolean modifyPW(String username, String newPassword) throws IOException, NoSuchUserException {
		User user = users.get(username);
		if (user == null) {
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
