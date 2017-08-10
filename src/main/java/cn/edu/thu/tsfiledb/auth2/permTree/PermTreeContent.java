package cn.edu.thu.tsfiledb.auth2.permTree;

import java.io.IOException;
import java.io.RandomAccessFile;

import cn.edu.thu.tsfiledb.auth2.manage.AuthConfig;

public abstract class PermTreeContent {
	public static final int PAGE_SIZE = AuthConfig.PAGE_SIZE; // 40KB by default
	// record means this object
	public static final int RECORD_SIZE = PAGE_SIZE - PermTreeHeader.RECORD_SIZE;

	public static PermTreeContent readObject(RandomAccessFile raf) throws IOException {
		return null;
	}

	public void writeObject(RandomAccessFile raf) throws IOException {
	}
}
