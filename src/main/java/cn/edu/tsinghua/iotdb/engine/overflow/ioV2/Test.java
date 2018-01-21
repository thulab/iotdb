package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import java.io.File;
import java.io.IOException;

public class Test {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		File file = new File("path");
		System.out.println(file.getPath());
		System.out.println(file.getParent());
		System.out.println(file.pathSeparator);
		System.out.println(file.separator);
		System.out.println(file.getAbsolutePath());
		System.out.println(file.getCanonicalPath());
		System.out.println(file.toPath());
	}

}
