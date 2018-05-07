package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.iotdb.utils.MemUtils;

public class MemTablePerf {

	public static void main(String[] args) {
		System.gc();
		Runtime runtime = Runtime.getRuntime();
		System.out.println(MemUtils.bytesCntToStr(runtime.totalMemory()));
		System.out.println(MemUtils.bytesCntToStr(runtime.maxMemory()));
		System.out.println(MemUtils.bytesCntToStr(runtime.freeMemory()));
		
	}
}
