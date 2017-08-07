package cn.edu.thu.tsfiledb.auth2;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.Test;

import cn.edu.thu.tsfiledb.auth2.exception.WriteObjectException;
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

}
