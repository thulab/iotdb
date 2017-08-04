package cn.edu.thu.tsfiledb.transferfile.transfer.client;

import cn.edu.thu.tsfiledb.transferfile.transfer.common.Md5CalculateUtil;
import cn.edu.thu.tsfiledb.transferfile.transfer.configure.ClientConfigure;

import java.io.*;
import java.net.Socket;
import java.util.Date;

/**
 * Created by dell on 2017/7/24.
 */
public class TransferFile extends Thread {
    private Socket socket;
    private String absolutePath;
    private String MD5;
    private long bytePosition;
    private final String messageSplitSig="\n";

    public TransferFile(Socket socket, String absolutePath,long bytePosition) {
        this.socket = socket;
        this.absolutePath = absolutePath.substring(0, absolutePath.length());
        this.bytePosition=bytePosition;
    }

    public void run() {
        InputStream ins=null;
        try {
            ins = socket.getInputStream();
            sendFileNameAndLength(absolutePath);
            byte[] input = new byte[1024];
            ins.read(input);
            boolean t = writeFileToServer(absolutePath,bytePosition);
            ins.read(input);
            t=t && MD5.equals(new String(input).split(messageSplitSig)[0]);

            System.out.println(new Date().toString() + " ------ finish send file " + new File(absolutePath).getName());

            if (t) {
                deleteFile(absolutePath);
            }
        }  catch (IOException e) {
            System.out.println("errors occur in socket InputStream or OutputStream!");
        }finally{
            if(ins!=null) try {
                ins.close();
            } catch (IOException e) {
                System.out.println("fail to close socket InputStream!");
            }
        }
    }

    private void sendFileNameAndLength(String absolutePath) throws IOException {
        File file = new File(absolutePath);
        OutputStream os = socket.getOutputStream();
        PrintWriter pw = new PrintWriter(os);
        pw.write(absolutePath + messageSplitSig + file.length() + messageSplitSig + bytePosition + messageSplitSig);
        pw.flush();
        os.flush();
    }

    private boolean writeFileToServer(String absolutePath,Long bytePosition) {
        boolean t = true;
        MD5= Md5CalculateUtil.getFileMD5(absolutePath);

        OutputStream os = null;
        try {
            os = socket.getOutputStream();
        } catch (IOException e) {
            e.printStackTrace();
        }
        File file = new File(absolutePath);
        FileInputStream in = null;
        try {
            in = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        byte[] buffer = new byte[Math.toIntExact(ClientConfigure.fileSegmentSize)];
        int size = 0;
        Long sendSize=Long.valueOf(0);
        try {
            while ((size = in.read(buffer)) != -1) {
                //System.out.println("sending package,size = " + size);
                sendSize+=size;
                if(sendSize<=bytePosition)continue;
                os.write(buffer, 0, size);
                os.flush();
                InputStream ins = socket.getInputStream();
                byte[] readAccept=new byte[128];
                ins.read(readAccept);
                String temp=new String(readAccept);
                bytePosition+=Long.parseLong(temp.split("\n")[0]);
                //TransferThread.setMap(absolutePath,bytePosition);
                updateBytePosition(absolutePath,bytePosition);
            }
        } catch (IOException e) {
        }finally{
            try {
                if(in!=null) in.close();
            } catch (IOException e) {
                System.out.println("fail to close FileInputStream while writing file "+absolutePath+" to server!");
            }
        }
        t=(bytePosition==file.length());
        return t;
    }

    private void updateBytePosition(String absolutePath, Long bytePosition) throws IOException {
        ObjectOutputStream oos = null;
        File file=new File(absolutePath);
        String filePath=ClientConfigure.filePositionRecord.concat("record_"+file.getName());

        try {
            oos = new ObjectOutputStream(new FileOutputStream(filePath));
            oos.writeObject(new FilePositionRecord(absolutePath,bytePosition));
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            oos.close();
        }
    }

    private static boolean deleteFile(String absolutePath) {
        boolean t = false;
        File file = new File(absolutePath);
        if (file.exists() && file.isFile()) {
            if (file.delete()) {
                System.out.println("delete file " + absolutePath + "success!");
                t = true;
            } else {
                System.out.println("delete file " + absolutePath + "fail!");
                t = false;
            }
        } else {
            System.out.println("delete file fail: " + absolutePath + "not exist!");
            t = false;
        }
        return t;
    }
}