package cn.edu.tsinghua.iotdb.engine;

import cn.edu.tsinghua.tsfile.compress.UnCompressor;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.Encoding;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.PageReader;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class PathUIDManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(PathUIDManager.class);
    private final int MAX_LENGTH = 15;
    private final String PATH_SEPARATOR = "\\.";
    private final String ROOT = "root";

    private int size;
    private List<Integer> lengthList;
    private List<Long> maskList;
    private List<Long> shiftList;
    private List<Long> indexRangeList;

    private List<List<String>> pathList;

    private PathUIDManager(){
        lengthList = new ArrayList<>();
        shiftList = new ArrayList<>();
        maskList = new ArrayList<>();
        indexRangeList = new ArrayList<>();

        pathList = new ArrayList<>();

        init();
    }

    private void init(){
        lengthList = Arrays.asList(5,5,5);
        int sum = 0;
        for(int length : lengthList)
            sum += length;
        if(sum > MAX_LENGTH){
            LOGGER.error("too much space required by uid");
        }

        size = lengthList.size();

        for(int i = 0;i < size;i++) {
            pathList.add(new ArrayList<>());
        }

        int start = MAX_LENGTH;
        for(int length : lengthList){
            start -= length;

            long shift = (long) Math.pow(16, start);
            shiftList.add(shift);

            long mask = 0;
            for(int i = 0;i < length;i++){
                mask = mask * 16 + 15;
            }
            indexRangeList.add(mask);
            maskList.add(mask * shift);
        }
    }

    private void checkIndexRange(long subUID, int index){
        if(subUID > indexRangeList.get(index)){
            LOGGER.error("too many subpath for %d level.", index + 1);
        }
    }

    private long insertSubPath(String subPath, int index){
        List<String> subPathList = pathList.get(index);
        for(int i = 0;i < subPathList.size();i++){
            if(subPathList.get(i).equals(subPath))return i;
        }
        subPathList.add(subPath);
        long subUID = subPathList.size() - 1;
        checkIndexRange(subUID, index);
        return subUID;
    }

    public long addPath(String path){
        if(path.startsWith(ROOT))path = path.substring((ROOT + PATH_SEPARATOR).length() - 1);
        List<String> subPathList = Arrays.asList(path.split(PATH_SEPARATOR));
        if(subPathList.size() != size){
            LOGGER.error("wrong subpath num of path %s.", path);
        }

        long UID = 0;
        for(int i = 0;i < size;i++){
            UID += insertSubPath(subPathList.get(i), i) * shiftList.get(i);
        }
        return UID;
    }

    private long getSubUID(long UID, int index){
        long subUID = UID & maskList.get(index);
        int shift = 0;
        for(int i = index + 1;i < size;i++){
            shift += lengthList.get(i);
        }
        subUID = subUID >> shift;
        return subUID;
    }

    public String getPath(long UID){
        String path = ROOT;
        for(int i = 0;i < size;i++) {
            String subPath = pathList.get(i).get((int) getSubUID(UID, i));
            path += "." + subPath;
        }
        return path;
    }

    public static void main(String[] args) throws IOException {
        System.out.println("start read file");
        List<String> pathList = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("/Users/East/Desktop/path_uid/path.txt"));
        String str = null;
//        while((str = br.readLine()) != null) {
        for(int i = 0;i < 10000;i++){
            str = br.readLine();
            pathList.add(str);
        }
        br.close();
        System.out.println("complete read file");

        System.out.println("start init");
        Map<String, Integer> pathMap1 = new HashMap<>();
        Map<Long, Integer> pathMap2 = new HashMap<>();
        List<Long> UIDList = new ArrayList<>();
        PathUIDManager manager = new PathUIDManager();
        for(int i = 0;i < pathList.size();i++) {
            pathMap1.put(pathList.get(i), i);
            long UID = manager.addPath(pathList.get(i));
            UIDList.add(UID);
            pathMap2.put(UID, i);
            System.out.println("init:" + (i / (float)pathList.size()) + "\t" + i);
        }
        System.out.println("complete init");

        System.out.println("start prove correctness");
        int wrongnum = 0;
        for(int i = 0;i < pathList.size();i++){
            System.out.println(i);
            System.out.println(Long.toHexString(UIDList.get(i)));
            if(!manager.getPath(UIDList.get(i)).equals(pathList.get(i)))wrongnum++;
        }
        System.out.println("wrong num:" + wrongnum);
        System.out.println("end prove correctness");

        System.out.println("start query");
        Random random = new Random(System.currentTimeMillis());
        long stime, ltime;
        stime = ltime = 0;
        long starttime, endtime, time;
        for(int i = 0;i < 100;i++){
            int index = random.nextInt(pathList.size());
            String path = pathList.get(index);
            long UID = UIDList.get(index);
            int value1, value2;
            value1 = 1;
            value2 = 2;

            starttime = System.currentTimeMillis();
            for(int j = 0;j < 1000000;j++) {
                value1 = pathMap1.get(path);
            }
            endtime = System.currentTimeMillis();
            time = endtime - starttime;
            stime += time;
            System.out.println(time);

            starttime = System.currentTimeMillis();
            for(int j = 0;j < 1000000;j++) {
                value2 = pathMap2.get(UID);
            }
            endtime = System.currentTimeMillis();
            time = endtime - starttime;
            ltime += time;
            System.out.println(time);

            System.out.println(value1 == value2);
        }
        System.out.println(stime);
        System.out.println(ltime);
        System.out.println("end query");
    }
}
