package cn.edu.thu.tsfiledb.transferfile;

import cn.edu.thu.tsfiledb.service.DataCollectClient;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileInfo;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameAllResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameResp;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lylw on 2017/7/28.
 */
public class TestGetFile {
    static String basePath="G:\\tsfiledb_test\\startTime\\";
    static String snapShotPath="G:\\testFile\\";
    public static void main(String[] args) {
        DataCollectClient client = new DataCollectClient("127.0.0.1", 6668);
        TSFileNodeNameAllResp tsFileNodeNameAllResp=client.getFileAllNode();
        List<String> fileNodeList=tsFileNodeNameAllResp.getFileNodesList();

        for(int i=0;i<fileNodeList.size();i++){
            String namespace=fileNodeList.get(i);

            Map<String,Long> startTimes=loadStartTimes(namespace);//namespace中 每个device startTime
//            System.out.println("startTimes");
            for (Map.Entry<String, Long> entry : startTimes.entrySet()) {
                System.out.println(entry.getKey()+" "+entry.getValue());
            }

            TSFileNodeNameResp tsFileNodeNameResp=client.getFileNode(namespace,startTimes,233333L);
            List<TSFileInfo> tsFileInfoList=tsFileNodeNameResp.getFileInfoList();//tsfileInfo

            for(int j=0;j<tsFileInfoList.size();j++){
                String tsFilePath=tsFileInfoList.get(j).getFilePath();//device dir
//                System.out.println("tsFilePath "+tsFilePath);
                copyFileSnapShot(tsFilePath,snapShotPath);
                updateStartTimes(namespace,tsFileInfoList.get(j).getEndTimes());
            }

            int token = tsFileNodeNameResp.getToken();
            System.out.println("token "+token);
            client.backFileNode(fileNodeList.get(i), tsFileInfoList, token);
        }
    }

    private static void copyFileSnapShot(String tsFilePath, String snapShotPath) {
        File inputFile=new File(tsFilePath);
        File outputFile=new File(snapShotPath.concat(inputFile.getName()));
        FileInputStream fis=null;
        FileOutputStream fos=null;
        try {
            fis=new FileInputStream(inputFile);
            fos=new FileOutputStream(outputFile);
            byte[] copyfile=new byte[1024];
            while(fis.read(copyfile)!=-1){
                fos.write(copyfile);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void updateStartTimes(String namespace, Map<String,Long> newStartTime) {
        ObjectOutputStream oos = null;
        String dirPath=basePath.concat(namespace+"\\");

        try {
            for (Map.Entry<String, Long> entry : newStartTime.entrySet()) {
                String filePath= dirPath.concat(entry.getKey()+".txt");
                File dir=new File(dirPath);
                if(!dir.exists())dir.mkdirs();
                oos = new ObjectOutputStream(new FileOutputStream(filePath));
                oos.writeObject(new StartTime(entry.getKey(),entry.getValue()+1));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(oos);
        }
    }

    private static Map<String,Long> loadStartTimes(String namespace) {
        Map<String,Long> startTimes=new HashMap<>();
        String path=basePath.concat(namespace);
        File dir=new File(path);
        ObjectInputStream ois = null;
        if(dir.exists()){
            File[] files = dir.listFiles();
            for (File file : files) {
                try {
                    ois = new ObjectInputStream(new FileInputStream(file));
                    StartTime startTime=(StartTime) ois.readObject();
                    startTimes.put(startTime.getDevice(),startTime.getStartTime());
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
        return startTimes;
    }
}