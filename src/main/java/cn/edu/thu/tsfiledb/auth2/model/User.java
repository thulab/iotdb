package cn.edu.thu.tsfiledb.auth2.model;

import java.io.IOException;
import java.io.RandomAccessFile;

import cn.edu.thu.tsfiledb.auth2.exception.ReadObjectException;
import cn.edu.thu.tsfiledb.auth2.exception.WriteObjectException;
import cn.edu.thu.tsfiledb.utils.SerializeUtils;

public class User{
	public static final int MAX_NAME_LENGTH = 256;
	public static final int MAX_PW_LENGTH = 64;
	public static final int RECORD_SIZE = MAX_NAME_LENGTH + MAX_PW_LENGTH + Integer.BYTES;
	
	private int ID;
	private String username; 
	private String password;
	
	public User() {
		
	}
	
	public User(String username, String password) {
		this.setUsername(username);
		this.setPassword(password);
	}
	
	public User(String username, String password, int newUserID) {
		this.setUsername(username);
		this.setPassword(password);
		this.setID(newUserID);
	}

	public static User readObject(RandomAccessFile raf) throws IOException, ReadObjectException {
		User user = new User();
		user.setID(raf.readInt());
		
		user.setUsername(SerializeUtils.readString(raf, MAX_NAME_LENGTH));
		
		user.setPassword(SerializeUtils.readString(raf, MAX_PW_LENGTH));
		return user;
	}
	
	public void writeObject(RandomAccessFile raf) throws IOException, WriteObjectException {
		raf.writeInt(ID);
		
		SerializeUtils.writeString(raf, username, MAX_NAME_LENGTH);
		
		SerializeUtils.writeString(raf, password, MAX_PW_LENGTH);
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
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
