package cn.edu.thu.tsfiledb.auth2;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.Test;

import cn.edu.thu.tsfiledb.auth2.exception.NoSuchUserException;
import cn.edu.thu.tsfiledb.auth2.exception.WriteObjectException;
import cn.edu.thu.tsfiledb.auth2.manage.UserManager;
import cn.edu.thu.tsfiledb.auth2.model.User;

public class UserTest {

	@Test
	public void serializeTest() throws IOException, ClassNotFoundException {
		User user1 = new User("1", "a");
		User user2 = new User("2", "b");
		User longName = new User("tooooooooooooooooooooooooooooooooloooooooongggggaaaaabbbbb"
								+ "ggggggggggggggggggggggggnammmmmmmmmmmmmmmmmmmmmbbbbbbbbb"
								+ "ggggggggggggggggggggggggnammmmmmmmmmmmmmmmmmmmmbbbbbbbbb"
								+ "ggggggggggggggggggggggggnammmmmmmmmmmmmmmmmmmmmbbbbbbbbb"
								+ "ggggggggggggggggggggggggnammmmmmmmmmmmmmmmmmmmmbbbbbbbbb"
								+ "ggggggggggggggggggggggggnammmmmmmmmmmmmmmmmmmmmbbbbbbbbb",
								"c");
		
		File testFile = new File("temp");
		RandomAccessFile fStream = new RandomAccessFile(testFile, "rw");
		user1.writeObject(fStream);
		user2.writeObject(fStream);
		
		boolean caught = false;
		try {
			longName.writeObject(fStream);
		} catch (Exception e) {
			if(e instanceof WriteObjectException)
				caught = true;
		}
		assertTrue(caught);
		fStream.close();
		
		RandomAccessFile iStream = new RandomAccessFile(testFile, "r");
		User nUser1 = User.readObject(iStream);
		User nUser2 = User.readObject(iStream);
		assertTrue(user1.getUsername().equals(nUser1.getUsername()));
		assertTrue(user2.getUsername().equals(nUser2.getUsername()));
		assertTrue(user1.getPassword().equals(nUser1.getPassword()));
		assertTrue(user2.getPassword().equals(nUser2.getPassword()));
		iStream.close();
		
		// test random read
		iStream = new RandomAccessFile(testFile, "r");
		iStream.seek(User.RECORD_SIZE);
		User nUser3 = User.readObject(iStream);
		assertTrue(user2.getUsername().equals(nUser3.getUsername()));
		assertTrue(user2.getPassword().equals(nUser3.getPassword()));
		iStream.close();
	}
	
	@Test
	public void createTest() throws IOException {
		UserManager manager = UserManager.getInstance();
		String username = "admin", password = "nimda";
		manager.createUser(username, password);
		
		assertTrue(manager.authorize(username, password));
		assertFalse(manager.createUser(username, password));
		assertFalse(manager.authorize(username, password + "wrong"));
		assertFalse(manager.authorize(username + "wrong", password));
	}
	
	@Test
	public void deleteTest() throws IOException {
		UserManager manager = UserManager.getInstance();
		String username = "admin2", password = "2nimda";
		manager.createUser(username, password);
		assertTrue(manager.authorize(username, password));
		
		manager.deleteUser(username);
		assertFalse(manager.authorize(username, password));
		assertTrue(manager.createUser(username, password));
	}
	
	@Test
	public void modifyPWTest() throws IOException, NoSuchUserException {
		UserManager manager = UserManager.getInstance();
		String username = "admin3", password = "3nimda";
		manager.deleteUser(username);
		manager.createUser(username, password);
		assertTrue(manager.authorize(username, password));
		
		String newPW = "3nimdawen";
		assertTrue(manager.modifyPW(username, newPW));
		assertFalse(manager.authorize(username, password));
		assertTrue(manager.authorize(username, newPW));
	}

}
