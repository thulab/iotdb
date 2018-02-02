package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import org.junit.Test;

import java.io.File;

import static cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType.INT32;

public class TreeSetTest {
    public static IMemTable testTreeSetMemTable(int deviceNum, int sensorNum, int seriesPointNum) {
        IMemTable memTable = new TreeSetMemTable();
        for(int i = 0; i < deviceNum; i++){
            for (int j = 0; j < sensorNum; j++) {
                memTable.addSeriesIfNone("d"+i,"s"+j, INT32);
            }
        }
        long time = Long.MAX_VALUE/2;
        int v = 0;
        for(int i = 0; i < deviceNum; i++){
//            System.out.println("device d"+i);
            for (int j = 0; j < sensorNum; j++) {
                for (int k = 0; k < seriesPointNum; k++) {
                    memTable.write("d"+i,"s"+j, INT32, time, v+"");
                    time--;
                    v++;
                }
            }
        }
        return memTable;
    }

    public static TimeValuePair[] testBased(int num) {
        TimeValuePair[] tvp = new TimeValuePair[num];
        for (int i = 0; i < num; i++) {
            tvp[i] = new TimeValuePair(i, new TsPrimitiveType.TsInt(i));
        }
        return tvp;
    }



    private static final Runtime s_runtime = Runtime.getRuntime ();
    private static long usedMemory (){
        s_runtime.gc();
        return s_runtime.totalMemory () - s_runtime.freeMemory ();
    }

    public static void main(String[] args) {
        System.out.println("la la la");
        int[] oneSeriesPointNum = {50, 100, 200, 500};
        for (int num : oneSeriesPointNum) {
            long mem = usedMemory();
            TimeValuePair[] tvp = testBased(num * 10000);
//            IMemTable memTable = testTreeSetMemTable(1, 1, num*10000);
            long mem2 = usedMemory();
            System.out.println(num+"\t"+(mem2-mem));

        }


//        int[] oneSeriesPointNum = {50, 60, 70, 80, 90, 100};
//        int[] oneSeriesPointNum = {10, 50, 100, 200, 500};
//        for (int num : oneSeriesPointNum) {
//            long mem = usedMemory();
//            IMemTable memTable = testTreeSetMemTable(1, 1, num*10000);
//            long mem2 = usedMemory();
////            System.out.println(mem);
////            System.out.println(mem2);
//            System.out.println(num+"\t"+(mem2-mem));
////            System.out.println("lala");
//        }


//        File[] roots = File.listRoots();
//        // 获取磁盘分区列表
//        for (File file : roots) {
//            System.out.println(file.getPath() + "信息如下:");
//            System.out.println("空闲未使用 = " + file.getFreeSpace() / 1024 / 1024
//                    / 1024 + "G");// 空闲空间
//            System.out.println("已经使用 = " + file.getUsableSpace() / 1024 / 1024
//                    / 1024 + "G");// 可用空间
//            System.out.println("总容量 = " + file.getTotalSpace() / 1024 / 1024
//                    / 1024 + "G");// 总空间
//            System.out.println();
//        }
//
//        //只能查看JAVA 虚拟机占用的内存
//        System.out.println( Runtime.getRuntime().maxMemory() / 1024 / 1024+"M"); //返回 Java 虚拟机试图使用的最大内存量。
//        System.out.println( Runtime.getRuntime().freeMemory() / 1024 / 1024 +"M");//返回 Java 虚拟机中的空闲内存量。
//        System.out.println(Runtime.getRuntime().totalMemory() / 1024 / 1024+"M");  //返回 Java 虚拟机中的内存总量。

    }
}
