package cn.edu.thu.tsfiledb.auth2.model;

import java.io.IOException;
import java.io.RandomAccessFile;

import cn.edu.thu.tsfiledb.auth2.exception.ReadObjectException;
import cn.edu.thu.tsfiledb.auth2.exception.WriteObjectException;
import cn.edu.thu.tsfiledb.conf.AuthConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.utils.SerializeUtils;

public class User {
	private static AuthConfig authConfig = TsfileDBDescriptor.getInstance().getConfig().authConfig;
	
	public static final int MAX_NAME_LENGTH = authConfig.MAX_USERNAME_LENGTH;
	public static final int MAX_PW_LENGTH = authConfig.MAX_PW_LENGTH;
	public static final int RECORD_SIZE = MAX_NAME_LENGTH + MAX_PW_LENGTH + Integer.BYTES;

	private int ID;
	private String userName;
	private String password;

	public User() {

	}

	public User(String userName, String password) {
		this.setUserName(userName);
		this.setPassword(password);
	}

	public User(String userName, String password, int newUserID) {
		this.setUserName(userName);
		this.setPassword(password);
		this.setID(newUserID);
	}

	public static User readObject(RandomAccessFile raf) throws IOException, ReadObjectException {
		User user = new User();
		user.setID(raf.readInt());

		user.setUserName(SerializeUtils.readString(raf, MAX_NAME_LENGTH));

		user.setPassword(SerializeUtils.readString(raf, MAX_PW_LENGTH));
		return user;
	}

	public void writeObject(RandomAccessFile raf) throws IOException, WriteObjectException {
		raf.writeInt(ID);

		SerializeUtils.writeString(raf, userName, MAX_NAME_LENGTH);

		SerializeUtils.writeString(raf, password, MAX_PW_LENGTH);
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getID() {
		return ID;
	}

	public void setID(int iD) {
		ID = iD;
	}
}
