package cn.edu.tsinghua.iotdb.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author liurui
 */

// Notice : methods in this class may not be accurate. Because limited user authority.

public class OpenFileNumUtil {
    private static Logger log = LoggerFactory.getLogger(OpenFileNumUtil.class);
    private static int pid = -1;
    private static String processName ;

    private static final String SEARCH_PID_LINUX = "ps -aux | grep -i %s | grep -v grep";
    private static final String SEARCH_PID_MAC = "ps aux | grep -i %s | grep -v grep";
    private static final String SEARCH_OPEN_DATA_FILE_BY_PID = "lsof -p %d";
    private static String cmds[] = {"/bin/bash", "-c", ""};
    private static OpenFileNumUtil INSTANCE = null;

    /**
     * 指定进程关键字的构造函数
     * @param pName 进程关键字
     */
    private OpenFileNumUtil(String pName) {
        processName = pName;
        pid = getPID();
    }

    /**
     * 不指定进程关键字的构造函数，默认进程关键字为IOTDB_HOME
     */
    private OpenFileNumUtil() {
        processName = "IOTDB_HOME";
        pid = getPID();
    }

    /**
     * 单例模式
     *
     * @return 单例对象
     */
    public static OpenFileNumUtil getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new OpenFileNumUtil();
        }
        return INSTANCE;
    }

    /**
     * 测试用的指定进程关键字的getInstance函数
     * @param pName
     * @return
     */
    public static OpenFileNumUtil getInstance(String pName) {
        if (INSTANCE == null) {
            INSTANCE = new OpenFileNumUtil(pName);
        }
        return INSTANCE;
    }
    /**
     * 获得IoTDB服务的PID
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
            //System.out.println(command);
            cmds[2] = command;
            pro1 = r.exec(cmds);
            BufferedReader in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
            String line = null;
            while ((line = in1.readLine()) != null) {
                line = line.trim();
                //System.out.println(line);
                String[] temp = line.split("\\s+");
                if (temp.length > 1 && isNumeric(temp[1])) {
                    pid = Integer.parseInt(temp[1]);
                    break;
                }
            }
            in1.close();
            pro1.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            log.error("统计打开文件数时getPid()发生异常： " + e.getMessage());
            log.error(sw.toString());
        }
        return pid;
    }

    /**
     * 获取进程id
     *
     * @return pid
     */
    public static int getPid() {
        return pid;
    }

    /**
     * 设置进程id
     *
     */
    public static void setPid(int pid) {
        OpenFileNumUtil.pid = pid;
    }

    /**
     * 获取进程关键字
     *
     * @return processName
     */
    public static String getProcessName() {
        return processName;
    }

    /**
     * 设置进程关键字
     *
     */
    public static void setProcessName(String processName) {
        OpenFileNumUtil.processName = processName;
    }

    /**
     * 检验一个字符串是否是整数
     *
     * @param str 需要判断是否为整数的字符串
     * @return
     */
    private static boolean isNumeric(String str) {
        for (int i = str.length(); --i >= 0; ) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 返回打开文件数的统计结果列表，其中：
     * list[0]表示当前IoTDB服务进程一共打开的文件数目
     * 1ist[1]表示当前IoTDB服务进程打开的/data路径下文件的数目
     * 1ist[2]表示当前IoTDB服务进程打开的/data/delta路径下文件的数目
     * 1ist[3]表示当前IoTDB服务进程打开的/data/overflow路径下文件的数目
     * 1ist[4]表示当前IoTDB服务进程打开的/data/wals路径下文件的数目
     * 1ist[5]表示当前IoTDB服务进程打开的/data/metadata路径下文件的数目
     * 1ist[6]表示当前IoTDB服务进程打开的/data/digest路径下文件的数目
     * 1ist[7]表示当前IoTDB服务进程打开的socket的数目
     *
     * @param pid 服务pid
     * @return list 统计结果
     * @throws SQLException SQL异常
     */
    private ArrayList<Integer> getOpenFile(int pid) throws SQLException {
        //log.info("开始收集打开的socket数目：");
        ArrayList<Integer> list = new ArrayList<Integer>();
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
                //System.out.println(line);
                String[] temp = line.split("\\s+");
                if (line.contains("" + pid) && temp.length > 8) {
                    totalFileNum++;
                    if (temp[8].contains("/data/")) {
                        dataFileNum++;
                    }
                    if (temp[7].contains("TCP") || temp[7].contains("UDP")) {
                        socketNum++;
                    }
                    if (temp[8].contains("/data/delta/")) {
                        deltaNum++;
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
                    if (temp[8].contains("/data/overflow/")) {
                        overflowNum++;
                    }
                }
            }
            in.close();
            pro.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            log.error("统计打开文件数时getOpenFile()发生InstantiationException. " + e.getMessage());
            log.error(sw.toString());
        }
        list.add(totalFileNum);
        list.add(dataFileNum);
        list.add(deltaNum);
        list.add(overflowNum);
        list.add(walNum);
        list.add(metadataNum);
        list.add(digestNum);
        list.add(socketNum);
        return list;
    }

    /**
     * 正确性检查并返回最终结果列表，若PID异常会返回-1，若操作系统不支持则返回-2
     * 若操作系统支持，则返回打开文件数的统计结果列表，其中：
     * list[0]表示当前IoTDB服务进程一共打开的文件数目
     * 1ist[1]表示当前IoTDB服务进程打开的/data路径下文件的数目
     * 1ist[2]表示当前IoTDB服务进程打开的/data/delta路径下文件的数目
     * 1ist[3]表示当前IoTDB服务进程打开的/data/overflow路径下文件的数目
     * 1ist[4]表示当前IoTDB服务进程打开的/data/wals路径下文件的数目
     * 1ist[5]表示当前IoTDB服务进程打开的/data/metadata路径下文件的数目
     * 1ist[6]表示当前IoTDB服务进程打开的/data/digest路径下文件的数目
     * 1ist[7]表示当前IoTDB服务进程打开的socket的数目
     * @return list
     */
    public ArrayList<Integer> get() {
        String os = System.getProperty("os.name").toLowerCase();
        ArrayList<Integer> list = null;
        //判断当前操作系统，目前仅支持Linux和Mac OS，其他Linux系的操作系统行为不明确，暂且认为也不支持
        if(os.startsWith("linux") || os.startsWith("mac")) {
            //如果pid不合理，再次尝试获取
            if (pid < 0) {
                pid = getPid();
            }
            //如果pid合理，则加入打开文件总数和数据文件数目以及socket数目
            if (pid > 0) {
                try {
                    list = getOpenFile(pid);
                } catch (SQLException e) {
                    log.error(e.getMessage());
                    e.printStackTrace();
                }
            } else {
                //pid 不合理，则全赋值为-1
                list = new ArrayList<Integer>();
                for (int i = 0; i < 8; i++) {
                    list.add(-1);
                }
            }
        } else {
            //操作系统不支持，则全赋值为-2
            list = new ArrayList<Integer>();
            for (int i = 0; i < 8; i++) {
                list.add(-2);
            }
        }
        return list;
    }
}
