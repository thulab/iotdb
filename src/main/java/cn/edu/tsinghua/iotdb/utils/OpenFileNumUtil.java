package cn.edu.tsinghua.iotdb.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * @author liurui
 */

// Notice : methods in this class may not be accurate because of limited user authority.
public class OpenFileNumUtil {
    private static Logger log = LoggerFactory.getLogger(OpenFileNumUtil.class);
    private static int pid = -1;
    private static String processName ;
    private static final String SEARCH_PID_LINUX = "ps -aux | grep -i %s | grep -v grep";
    private static final String SEARCH_PID_MAC = "ps aux | grep -i %s | grep -v grep";
    private static final String SEARCH_OPEN_DATA_FILE_BY_PID = "lsof -p %d";
    private static String cmds[] = {"/bin/bash", "-c", ""};
    private static OpenFileNumUtil INSTANCE = null;
    public enum OpenFileNumStatistics {
        TOTAL_OPEN_FILE_NUM,
        DATA_OPEN_FILE_NUM,
        DELTA_OPEN_FILE_NUM,
        OVERFLOW_OPEN_FILE_NUM,
        WAL_OPEN_FILE_NUM,
        METADATA_OPEN_FILE_NUM,
        DIGEST_OPEN_FILE_NUM,
        SOCKET_OPEN_FILE_NUM
    }

    /**
     * constructor, default process key word is "IOTDB_HOME"
     */
    private OpenFileNumUtil() {
        processName = "IOTDB_HOME";
        pid = getPID();
    }

    /**
     * one instance
     *
     * @return instance
     */
    public static OpenFileNumUtil getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new OpenFileNumUtil();
        }
        return INSTANCE;
    }

    /**
     * get process ID by executing command
     * @return  pid
     *
     */
    public int getPID() {
        int pid = -1;
        Process pro1;
        Runtime r = Runtime.getRuntime();
        String os = System.getProperty("os.name").toLowerCase();

        try {
            String command ;
            if(os.startsWith("linux")) {
                command = String.format(SEARCH_PID_LINUX, processName);
            } else {
                command = String.format(SEARCH_PID_MAC, processName);
            }
            cmds[2] = command;
            pro1 = r.exec(cmds);
            BufferedReader in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
            String line = null;
            while ((line = in1.readLine()) != null) {
                line = line.trim();
                String[] temp = line.split("\\s+");
                if (temp.length > 1 && isNumeric(temp[1])) {
                    pid = Integer.parseInt(temp[1]);
                    break;
                }
            }
            in1.close();
            pro1.destroy();
        } catch (IOException e) {
            log.error("execute getPid() catch IOException：" + e.getMessage());
        }
        return pid;
    }

    /**
     * set id
     *
     */
    public static void setPid(int pid) {
        OpenFileNumUtil.pid = pid;
    }

    /**
     * check if a string is numeric
     *
     * @param str string need to be checked
     * @return
     */
    private static boolean isNumeric(String str) {
        if(str == null || str.equals("")){
            return false;
        }else {
            for (int i = str.length(); --i >= 0; ) {
                if (!Character.isDigit(str.charAt(i))) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * return statistic Map，whose key is belong to enum OpenFileNumStatistics：
     * TOTAL_OPEN_FILE_NUM is the current total open file number of IoTDB service process
     * DATA_OPEN_FILE_NUM is the current open file number under path '/data/delta' of IoTDB service process
     * DELTA_OPEN_FILE_NUM is the current open file number under path '/data/delta' of IoTDB service process
     * OVERFLOW_OPEN_FILE_NUM is the current open file number under path '/data/overflow' of IoTDB service process
     * WAL_OPEN_FILE_NUM is the current open file number under path '/data/wals' of IoTDB service process
     * METADATA_OPEN_FILE_NUM is the current open file number under path '/data/metadata' of IoTDB service process
     * DIGEST_OPEN_FILE_NUM is the current open file number under path '/data/digest' of IoTDB service process
     * SOCKET_OPEN_FILE_NUM is the current open socket connection of IoTDB service process
     *
     * @param pid : IoTDB service pid
     * @return list : statistics list
     * @throws SQLException SQL Exception
     */
    private HashMap<OpenFileNumStatistics, Integer> getOpenFile(int pid) {
        HashMap<OpenFileNumStatistics, Integer> resultMap = new HashMap<>();
        int dataFileNum = 0;
        int totalFileNum = 0;
        int socketNum = 0;
        int deltaNum = 0;
        int walNum = 0;
        int overflowNum = 0;
        int metadataNum = 0;
        int digestNum = 0;

        Process pro;
        Runtime r = Runtime.getRuntime();
        try {
            String command = String.format(SEARCH_OPEN_DATA_FILE_BY_PID, pid);
            cmds[2] = command;
            pro = r.exec(cmds);
            BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;

            while ((line = in.readLine()) != null) {
                String[] temp = line.split("\\s+");
                if (line.contains("" + pid) && temp.length > 8) {
                    totalFileNum++;
                    if (temp[8].contains("/data/")) {
                        dataFileNum++;
                    }
                    if (temp[8].contains("/data/delta/")) {
                        deltaNum++;
                    }
                    if (temp[8].contains("/data/overflow/")) {
                        overflowNum++;
                    }
                    if (temp[8].contains("/data/wals/")) {
                        walNum++;
                    }
                    if (temp[8].contains("/data/metadata/")) {
                        metadataNum++;
                    }
                    if (temp[8].contains("/data/digest/")) {
                        digestNum++;
                    }
                    if (temp[7].contains("TCP") || temp[7].contains("UDP")) {
                        socketNum++;
                    }
                }
            }
            in.close();
            pro.destroy();
        } catch (IOException e) {
            log.error("execute getOpenFile() catch IOException: {} " , e.getMessage());
        }
        resultMap.put(OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM, totalFileNum);
        resultMap.put(OpenFileNumStatistics.DATA_OPEN_FILE_NUM, dataFileNum);
        resultMap.put(OpenFileNumStatistics.DELTA_OPEN_FILE_NUM, deltaNum);
        resultMap.put(OpenFileNumStatistics.OVERFLOW_OPEN_FILE_NUM, overflowNum);
        resultMap.put(OpenFileNumStatistics.WAL_OPEN_FILE_NUM, walNum);
        resultMap.put(OpenFileNumStatistics.METADATA_OPEN_FILE_NUM, metadataNum);
        resultMap.put(OpenFileNumStatistics.DIGEST_OPEN_FILE_NUM, digestNum);
        resultMap.put(OpenFileNumStatistics.SOCKET_OPEN_FILE_NUM, socketNum);
        return resultMap;
    }

    /**
     * Check if runtime OS is supported then return the result list.
     * If pid is abnormal then all statistics returns -1, if OS is not supported then all statistics returns -2
     * @return list
     */
    public HashMap<OpenFileNumStatistics,Integer> get() {
        HashMap<OpenFileNumStatistics,Integer> resultMap = new HashMap<>();
        String os = System.getProperty("os.name").toLowerCase();
        //get runtime OS name, currently only support Linux and MacOS
        if(os.startsWith("linux") || os.startsWith("mac")) {
            //if pid is normal，then get statistics
            if (pid > 0) {
                resultMap = getOpenFile(pid);
            } else {
                //pid is abnormal, give all statistics abnormal value -1
                for(OpenFileNumStatistics statistics : OpenFileNumStatistics.values()){
                    resultMap.put(statistics,-1);
                }
            }
        } else {
            //operation system not supported, give all statistics abnormal value -2
            for(OpenFileNumStatistics statistics : OpenFileNumStatistics.values()){
                resultMap.put(statistics,-2);
            }
        }
        return resultMap;
    }

}
