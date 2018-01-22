package cn.edu.tsinghua.iotdb.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileUtils {

    public static void fileCopy(File from, File to) throws Exception{
        int length = 2 * 1024 * 1024;
        FileInputStream in = new FileInputStream(from);
        FileOutputStream out = new FileOutputStream(to);
        FileChannel inC = in.getChannel();
        FileChannel outC = out.getChannel();
        ByteBuffer b = null;
        while(true){
            if(inC.position() == inC.size()){
                inC.close();
                outC.close();
                return;
            }
            if((inC.size() - inC.position())< length){
                length =(int)(inC.size()-inC.position());
            }else
                length = 2 * 1024 * 1024;
            b = ByteBuffer.allocateDirect(length);
            inC.read(b);
            b.flip();
            outC.write(b);
            outC.force(false);
        }
    }

    public static void recurrentDelete(File file) {
        if(!file.exists())
            return;
        if(file.isDirectory()) {
            File[] files = file.listFiles();
            for (File subFile : files)
                recurrentDelete(subFile);
        }
        file.delete();
    }
}
