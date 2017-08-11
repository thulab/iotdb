package cn.edu.thu.tsfiledb.auth2.permTree;

import java.io.IOException;
import java.io.RandomAccessFile;

import cn.edu.thu.tsfiledb.conf.AuthConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;

public abstract class PermTreeContent {
	
	private static AuthConfig authConfig = TsfileDBDescriptor.getInstance().getConfig().authConfig;
	
	public static final int PAGE_SIZE = authConfig.PAGE_SIZE; // 40KB by default
	// record means this object
	public static final int RECORD_SIZE = PAGE_SIZE - PermTreeHeader.RECORD_SIZE;

	public static PermTreeContent readObject(RandomAccessFile raf) throws IOException {
		return null;
	}

	public abstract void writeObject(RandomAccessFile raf) throws IOException;
}
