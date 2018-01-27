package cn.edu.tsinghua.iotdb.utils;

import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class OpenFileNumUtilTest {

    @Test
    public void testTotalOpenFileNum(){
        OpenFileNumUtil openFileNumUtil = OpenFileNumUtil.getInstance();
        OpenFileNumUtil.setPid(getProcessID());
        String currDir = System.getProperty("user.dir");
        ArrayList<File> fileList = new ArrayList<>();
        int testTime = 10 ;
        int testFileNum;
        ArrayList<FileWriter> fileWriterList = new ArrayList<>();
        //ArrayList<FileReader> fileReaderList = new ArrayList<>();
        ArrayList<Integer> openFileNumResultList;
        int totalOpenFileNumBefore;
        int totalOpenFileNumAfter;
        int totalOpenFileNumChange;

        for(int test=0; test < testTime; test++) {
            //随机测试testTime次，每次创建随机的testFileNum个文件进行测试
            testFileNum = (int) ((Math.random() * 100) + 2);
            //初始状态统计打开文件数
            openFileNumResultList = openFileNumUtil.get();
            totalOpenFileNumBefore = openFileNumResultList.get(0);
            for (int i = 0; i < testFileNum; i++) {
                fileList.add(new File(currDir + "/testFileForOpenFileNumUtil" + i));
            }
            //创建testFileNum个File对象后，统计打开文件数
            openFileNumResultList = openFileNumUtil.get();
            totalOpenFileNumAfter = openFileNumResultList.get(0);
            totalOpenFileNumChange = totalOpenFileNumAfter - totalOpenFileNumBefore;
            //创建testFileNum个File对象应该不影响打开文件数
            assertEquals("0", String.valueOf(totalOpenFileNumChange));

            openFileNumResultList = openFileNumUtil.get();
            totalOpenFileNumBefore = openFileNumResultList.get(0);
            for (File file : fileList) {
                if (file.exists()) {
                    try {
                        fileWriterList.add(new FileWriter(file));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        file.createNewFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    try {
                        fileWriterList.add(new FileWriter(file));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            openFileNumResultList = openFileNumUtil.get();
            totalOpenFileNumAfter = openFileNumResultList.get(0);
            totalOpenFileNumChange = totalOpenFileNumAfter - totalOpenFileNumBefore;
            //创建testFileNum个FileWriter对象应该增加testFileNum个打开文件数
            assertEquals(String.valueOf(testFileNum), String.valueOf(totalOpenFileNumChange));


            openFileNumResultList = openFileNumUtil.get();
            totalOpenFileNumBefore = openFileNumResultList.get(0);
            for (FileWriter fw : fileWriterList) {
                try {
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            openFileNumResultList = openFileNumUtil.get();
            totalOpenFileNumAfter = openFileNumResultList.get(0);
            totalOpenFileNumChange = totalOpenFileNumAfter - totalOpenFileNumBefore;
            //关闭testFileNum个FileWriter对象应该减少testFileNum个打开文件数
            assertEquals(String.valueOf(-testFileNum), String.valueOf(totalOpenFileNumChange));

            //删除testFileNum个测试文件
            for (File file : fileList) {
                if (file.exists()) {
                    file.delete();
                }
            }
        }

    }

    public static void main(String args){
        //In this test only total we should care about.
        String[] statistic = {"total:","data:","delta:","overflow:","wals:","metadata:","digest:","socket:"};
        OpenFileNumUtil openFileNumUtil = OpenFileNumUtil.getInstance();
        OpenFileNumUtil.setPid(getProcessID());
        String currDir = System.getProperty("user.dir");
        ArrayList<File> fileList = new ArrayList<>();
        int testFileNum = 7;
        ArrayList<FileWriter> fileWriterList = new ArrayList<>();
        ArrayList<FileReader> fileReaderList = new ArrayList<>();
        ArrayList<Integer> getl0 ;

        //初始状态统计打开文件数
        getl0= openFileNumUtil.get();
        System.out.println("stage0");
        for(int i =0; i < getl0.size(); i++){
            System.out.println(statistic[i]+getl0.get(i));
        }

        for(int i = 0;i<testFileNum;i++){
            fileList.add(new File(currDir + "/testFileForOpenFileNumUtil" + i));
        }

        //创建testFileNum个File对象后，统计打开文件数
        getl0= openFileNumUtil.get();
        System.out.println("stage1");
        for(int i =0; i < getl0.size(); i++){
            System.out.println(statistic[i]+getl0.get(i));
        }

        int c = 0;
        for(File file : fileList){
            if(file.exists()){
                try {
                    fileWriterList.add(new FileWriter(file));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else{
                try {
                    boolean b = file.createNewFile();
                    if(b){
                        c++;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    fileWriterList.add(new FileWriter(file));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        //创建testFileNum个FileWriter对象后，统计打开文件数
        getl0= openFileNumUtil.get();
        System.out.println("stage2");
        for(int i =0; i < getl0.size(); i++){
            System.out.println(statistic[i]+getl0.get(i));
        }

        for(FileWriter fw : fileWriterList){
            try {
                fw.write("this is a test file for open file number counting.");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //testFileNum个FileWriter写文件后，统计打开文件数
        getl0= openFileNumUtil.get();
        System.out.println("stage3");
        for(int i =0; i < getl0.size(); i++){
            System.out.println(statistic[i]+getl0.get(i));
        }

        for(FileWriter fw : fileWriterList){
            try {
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //testFileNum个FileWriter关闭后，统计打开文件数
        getl0= openFileNumUtil.get();
        System.out.println("stage4");
        for(int i =0; i < getl0.size(); i++){
            System.out.println(statistic[i]+getl0.get(i));
        }

        for(File file : fileList){
            if(file.exists()){
                try {
                    fileReaderList.add(new FileReader(file));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else{
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    fileReaderList.add(new FileReader(file));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        //创建testFileNum个FileReader对象后，统计打开文件数
        getl0= openFileNumUtil.get();
        System.out.println("stage5");
        for(int i =0; i < getl0.size(); i++){
            System.out.println(statistic[i]+getl0.get(i));
        }

        for(FileReader fr : fileReaderList){
            try {
                fr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //testFileNum个FileReader关闭后，统计打开文件数
        getl0= openFileNumUtil.get();
        System.out.println("stage6");
        for(int i =0; i < getl0.size(); i++){
            System.out.println(statistic[i]+getl0.get(i));
        }

        //删除testFileNum个测试文件
        for(File file : fileList){
            if(file.exists()){
                file.delete();
            }
        }

    }

    private static int getProcessID() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        return Integer.parseInt(runtimeMXBean.getName().split("@")[0]);
    }

}
