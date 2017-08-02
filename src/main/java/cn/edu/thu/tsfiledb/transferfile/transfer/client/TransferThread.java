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
    private static Map<String,Long> filemap=new HashMap<>();
    static String startTimePath=ClientConfigure.startTimePath;
    static String snapShotPath=ClientConfigure.snapshootDirectory;

    public void run() {
        File file = new File(ClientConfigure.snapshootDirectory);
        File[] files = file.listFiles();
        Long curTime=System.currentTimeMillis();
        if(Client.isTimerTaskRunning() && files.length>0){
            System.out.println("Still transferring");
            return;
        }
        Client.setTimerTaskRunning(true);
        if(files.length==0){
            //请求文件列表，存储到 snapshootDirectory
            try {
                getFileFromDB();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            System.out.println(new Date().toString() + " ------ transfer files");
            writeFilesToServer(ClientConfigure.snapshootDirectory);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void getFileFromDB() throws IOException {
        DataCollectClient client = new DataCollectClient("127.0.0.1", 6668);
        TSFileNodeNameAllResp tsFileNodeNameAllResp=client.getFileAllNode();
        List<String> fileNodeList=tsFileNodeNameAllResp.getFileNodesList();

        for(int i=0;i<fileNodeList.size();i++){
            String namespace=fileNodeList.get(i);
            Map<String,Long> startTimes=loadStartTimes(namespace);//namespace中 每个device startTime

            System.out.println("show start times");
            for(Map.Entry<String,Long> entry:startTimes.entrySet()){
                System.out.println(entry.getKey()+" "+entry.getValue());
            }
            TSFileNodeNameResp tsFileNodeNameResp=client.getFileNode(namespace,startTimes,System.currentTimeMillis());
            int token=tsFileNodeNameResp.getToken();
            List<TSFileInfo> tsFileInfoList=tsFileNodeNameResp.getFileInfoList();//tsfileInfo

            for(int j=0;j<tsFileInfoList.size();j++){
                String tsFilePath=tsFileInfoList.get(j).getFilePath();//device dir
                copyFileSnapShot(tsFilePath,snapShotPath);
                updateStartTimes(namespace,tsFileInfoList.get(j).getEndTimes());
                Map<String,Long> endTimeMap=tsFileInfoList.get(j).getEndTimes();
            }
            client.backFileNode(namespace, tsFileInfoList, token);
        }
        client.DataCollectClientClose();
    }

    private static void copyFileSnapShot(String tsFilePath, String snapShotPath) throws IOException {
        System.out.println("copy files");
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
        }finally{
            fis.close();
            fos.close();
        }
    }

    private static void updateStartTimes(String namespace, Map<String,Long> newStartTime) {
        ObjectOutputStream oos = null;
        String dirPath=startTimePath.concat(namespace+"\\");

        try {
            for (Map.Entry<String, Long> entry : newStartTime.entrySet()) {
                String filePath= dirPath.concat(entry.getKey()+".txt");
                File dir=new File(dirPath);
                if(!dir.exists())dir.mkdirs();
                oos = new ObjectOutputStream(new FileOutputStream(filePath));
                oos.writeObject(new StartTime(entry.getKey(),entry.getValue()+1));
            }
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(oos);

        }
    }

    private static Map<String,Long> loadStartTimes(String namespace) {
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
                    ois.close();
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

    public static void setMap(String file,Long bytePosition){
        filemap.put(file,bytePosition);
    }

    public static void writeFilesToServer(String path) throws IOException {
        File file = new File(path);
        File[] files = file.listFiles();
        loadFileRecord(files);
        for (File file2 : files) {
            setMap(file2.getAbsolutePath(),Long.valueOf(0));
        }

        while (file.exists() && files.length>0) {//thread中断，server可连接
            ExecutorService fixedThreadPool = Executors.newFixedThreadPool(ClientConfigure.clientNTread);
            for (File file2 : files) {
                System.out.println(new Date().toString() + " ------ transfer a file " + file2.getName());
                try {
                    Socket socket = new Socket(ClientConfigure.server_address, ClientConfigure.port);//1024-65535的某个端口
                    System.out.println("socket success");
                    fixedThreadPool.submit(new MyThread(socket, file2.getAbsolutePath(), filemap.get(file2.getAbsolutePath())));
                }catch (IOException e){
                }
            }
            fixedThreadPool.shutdown();

            while(!fixedThreadPool.isTerminated());

            fixedThreadPool.shutdownNow();
            file = new File(path);
            files = file.listFiles();
            fixedThreadPool.shutdown();
        }
    }

    private static void loadFileRecord(File[] files) throws IOException {
        for(File file2 : files){
            String absolutePath = ClientConfigure.filePositionRecord.concat("record_"+file2.getName());
            File file=new File(absolutePath);
            if(file.exists()){
                ObjectInputStream ois = null;
                try {
                    ois = new ObjectInputStream(new FileInputStream(file));
                    FilePositionRecord filePositionRecord=(FilePositionRecord) ois.readObject();
                    setMap(filePositionRecord.getAbsolutePath(),filePositionRecord.getBytePosition());
                } catch (IOException e) {
                    setMap(file2.getAbsolutePath(),0L);
                } catch (ClassNotFoundException e) {
                    setMap(file2.getAbsolutePath(),0L);
                }finally{
                    ois.close();
                }

            }else{
                setMap(file2.getAbsolutePath(),0L);
            }
        }
    }
}