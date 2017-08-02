package cn.edu.thu.tsfiledb.transferfile.transfer.client;

import cn.edu.thu.tsfiledb.transferfile.transfer.configure.ClientConfigure;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by lylw on 2017/8/2.
 */
public class TestTransferThread extends TimerTask{
    private static Map<String,Long> filemap=new HashMap<>();

    public void run() {
        try {
            System.out.println(new Date().toString() + "test client ------ transfer files");
            writeFilesToServer("G:\\testfile1\\");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeFilesToServer(String path) throws IOException {
        File file = new File(path);
        File[] files = file.listFiles();
        System.out.println(files.length);
        while (file.exists() && files.length>0) {
            ExecutorService fixedThreadPool = Executors.newFixedThreadPool(ClientConfigure.clientNTread);
            for (File file2 : files) {
                System.out.println(new Date().toString() + " ------ transfer a file " + file2.getName());
                try {
                    Socket socket = new Socket(ClientConfigure.server_address, ClientConfigure.port);//1024-65535的某个端口
                    System.out.println("test client1 socket success");
                    fixedThreadPool.submit(new TransferFile(socket, file2.getAbsolutePath(), 0L));
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
}
