package cn.edu.thu.tsfiledb.transferfile.transfer.client;

import cn.edu.thu.tsfiledb.service.DataCollectClient;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileInfo;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameAllResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameResp;
import cn.edu.thu.tsfiledb.transferfile.transfer.configure.ClientConfigure;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by dell on 2017/7/25.
 */
public class TransferThread extends java.util.TimerTask {
    private String startTimePath=ClientConfigure.startTimePath;
    private String snapShotPath=ClientConfigure.snapshotDirectory;

    public void run() {
        File file = new File(ClientConfigure.snapshotDirectory);
        File[] files = file.listFiles();
        if(Client.isTimerTaskRunning() && files.length>0){
            System.out.println("Still transferring");
            return;
        }
        Client.setTimerTaskRunning(true);
        if(files.length==0){
            /**request for files,store in snapshot Directory*/
            getFileFromDB();
        }
        System.out.println(new Date().toString() + " ------ transfer files");
        writeFilesToServer(ClientConfigure.snapshotDirectory);
    }
    public void getFileFromDB() {
        DataCollectClient client = new DataCollectClient(ClientConfigure.readDBHost, ClientConfigure.readDBPort);
        TSFileNodeNameAllResp tsFileNodeNameAllResp=client.getFileAllNode();
        List<String> fileNodeList=tsFileNodeNameAllResp.getFileNodesList();

        for(int i=0;i<fileNodeList.size();i++){
            String namespace=fileNodeList.get(i);
            Map<String,Long> startTimes=loadStartTimes(namespace);
            /**show start times for every device in nameSpace*/
            System.out.println("show start times");
            for(Map.Entry<String,Long> entry:startTimes.entrySet()){
                System.out.println(entry.getKey()+" start time: "+entry.getValue());
            }
            TSFileNodeNameResp tsFileNodeNameResp=client.getFileNode(namespace,startTimes,System.currentTimeMillis());
            int token=tsFileNodeNameResp.getToken();
            List<TSFileInfo> tsFileInfoList=tsFileNodeNameResp.getFileInfoList();//tsFileInfo

            for(int j=0;j<tsFileInfoList.size();j++){
                String tsFilePath=tsFileInfoList.get(j).getFilePath();//device dir
                copyFileSnapShot(tsFilePath,snapShotPath);
                updateStartTimes(namespace,tsFileInfoList.get(j).getEndTimes());
            }
            client.backFileNode(namespace, tsFileInfoList, token);
        }
        client.DataCollectClientClose();
    }

    private void copyFileSnapShot(String tsFilePath, String snapShotPath) {
        System.out.println("copy file from tsFilePath to snapShotPath...");
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
            System.out.println("no file to copy...");
        } catch (IOException e) {
            System.out.println("errors occur while copying file");
        }finally{
            try {
                if(fis!=null) fis.close();
                if(fos!=null) fos.close();
            } catch (IOException e) {
                System.out.println("fail to close file after copy!");
            }
        }
    }

    private void updateStartTimes(String namespace, Map<String,Long> newStartTime) {
        ObjectOutputStream oos = null;
        String dirPath=startTimePath.concat(namespace+System.getProperty("file.separator"));

        for (Map.Entry<String, Long> entry : newStartTime.entrySet()) {
            try {
                String filePath= dirPath.concat(entry.getKey());
                File dir=new File(dirPath);
                if(!dir.exists())dir.mkdirs();
                oos = new ObjectOutputStream(new FileOutputStream(filePath));
                oos.writeObject(new StartTime(entry.getKey(),entry.getValue()+1));
            }catch (IOException e) {
                System.out.println("update startTime for device "+entry.getKey()+" fail!");
            } finally {
                IOUtils.closeQuietly(oos);
            }
        }
    }

    private Map<String,Long> loadStartTimes(String namespace) {
        Map<String,Long> startTimes=new HashMap<>();
        String path=startTimePath.concat(namespace);
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
                } catch (IOException e) {
                    startTimes.put(file.getName(),0L);
                } catch (ClassNotFoundException e) {
                    startTimes.put(file.getName(),0L);
                }finally{
                    if(ois!=null) try {
                        ois.close();
                    } catch (IOException e) {
                        System.out.println("fail to close file "+file.getName());
                    }
                }
            }
        }
        return startTimes;
    }

    public void writeFilesToServer(String path) {
        File file = new File(path);
        File[] files = file.listFiles();

        while (file.exists() && files.length>0) {/**thread interruption,file re_transfer*/
            ExecutorService fixedThreadPool = Executors.newFixedThreadPool(ClientConfigure.clientNTread);
            for (File traverseFile : files) {
                System.out.println(new Date().toString() + " ------ transfer a file " + traverseFile.getName());
                try {
                    Socket socket = new Socket(ClientConfigure.server_address, ClientConfigure.port);//port from 1024-65535
                    System.out.println("socket success");

                    fixedThreadPool.submit(new TransferFile(socket, traverseFile.getAbsolutePath(),getFileBytePosition(traverseFile.getAbsolutePath())));
                }catch (IOException e){
                }
            }
            fixedThreadPool.shutdown();

            while(!fixedThreadPool.isTerminated()) {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                }
            }

            fixedThreadPool.shutdownNow();
            file = new File(path);
            files = file.listFiles();
            fixedThreadPool.shutdown();
        }
    }

    private long getFileBytePosition(String filePath) {
        long bytePosition=0;
        File file=new File(filePath);
        String fileRecordPath = ClientConfigure.filePositionRecord.concat("record_"+file.getName());

        ObjectInputStream ois = null;
        ObjectOutputStream oos= null;
        File recordFile=new File(fileRecordPath);
        if(recordFile.exists()){
            try{
                ois = new ObjectInputStream(new FileInputStream(recordFile));
                FilePositionRecord filePositionRecord=(FilePositionRecord) ois.readObject();
                return filePositionRecord.bytePosition;
            } catch (FileNotFoundException e) {
                return 0L;
            } catch (IOException e) {
                try {
                    oos = new ObjectOutputStream(new FileOutputStream(recordFile));
                    oos.writeObject(new FilePositionRecord(file.getAbsolutePath(),0L));
                } catch (IOException e1) {
                    System.out.println("fail to rewrite recordFile...");
                }
                return 0L;
            } catch (ClassNotFoundException e) {
                try {
                    oos = new ObjectOutputStream(new FileOutputStream(recordFile));
                    oos.writeObject(new FilePositionRecord(file.getAbsolutePath(),0L));
                } catch (IOException e1) {
                    System.out.println("fail to read recordFile...");
                }
                return 0L;
            }
        }
        else{
            try {
                recordFile.createNewFile();
                oos = new ObjectOutputStream(new FileOutputStream(recordFile));
                oos.writeObject(new FilePositionRecord(file.getAbsolutePath(),0L));
            } catch (IOException e) {
                e.printStackTrace();
            }finally{
                try {
                    if(oos!=null) oos.close();
                } catch (IOException e) {
                    System.out.println("fail to close recordFile!");
                }
            }
        }
        return bytePosition;
    }
}