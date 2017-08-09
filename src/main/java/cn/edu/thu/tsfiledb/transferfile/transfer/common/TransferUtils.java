package cn.edu.thu.tsfiledb.transferfile.transfer.common;

import java.io.File;

public class TransferUtils {
	public static boolean deleteFile(String absolutePath) {
		boolean t = false;
		File file = new File(absolutePath);
		if (file.exists() && file.isFile()) {
			if (file.delete()) {
				System.out.println("delete file " + absolutePath + " success!");
				t = true;
			} else {
				System.out.println("delete file " + absolutePath + " fail!");
				t = false;
			}
		} else {
			System.out.println("delete file fail: " + absolutePath + " not exist!");
			t = false;
		}
		return t;
	}
}
