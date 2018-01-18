package cn.edu.tsinghua.iotdb.engine.memtable;

import org.junit.Test;

import java.io.File;

public class TreeSetTest {
    @Test
    public void testTreeSetMemTable() {
        System.gc();

    }

    private static final Runtime s_runtime = Runtime.getRuntime ();
    private static long usedMemory (){
        return s_runtime.totalMemory () - s_runtime.freeMemory ();
    }
    private static void runGC () throws Exception{
        long usedMem1 = usedMemory (), usedMem2 = Long.MAX_VALUE;
        for (int i = 0; (usedMem1 < usedMem2) && (i < 500); ++ i){
            s_runtime.runFinalization ();
            s_runtime.gc ();
            Thread.currentThread ().yield ();
            usedMem2 = usedMem1;
            usedMem1 = usedMemory();
        }
    }

    public static void main(String[] args) {
        System.out.println("asdasds");
        File[] roots = File.listRoots();
        // 获取磁盘分区列表
        for (File file : roots) {
            System.out.println(file.getPath() + "信息如下:");
            System.out.println("空闲未使用 = " + file.getFreeSpace() / 1024 / 1024
                    / 1024 + "G");// 空闲空间
            System.out.println("已经使用 = " + file.getUsableSpace() / 1024 / 1024
                    / 1024 + "G");// 可用空间
            System.out.println("总容量 = " + file.getTotalSpace() / 1024 / 1024
                    / 1024 + "G");// 总空间
            System.out.println();
        }

        //只能查看JAVA 虚拟机占用的内存
        System.out.println( Runtime.getRuntime().maxMemory() / 1024 / 1024+"M"); //返回 Java 虚拟机试图使用的最大内存量。
        System.out.println( Runtime.getRuntime().freeMemory() / 1024 / 1024 +"M");//返回 Java 虚拟机中的空闲内存量。
        System.out.println(Runtime.getRuntime().totalMemory() / 1024 / 1024+"M");  //返回 Java 虚拟机中的内存总量。

    }
}
