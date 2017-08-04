package cn.edu.thu.tsfiledb.transferfile.transfer;

import cn.edu.thu.tsfiledb.transferfile.transfer.client.TransferThread;
import cn.edu.thu.tsfiledb.transferfile.transfer.conf.ClientConfig;

import java.io.IOException;
import java.util.Scanner;
import java.util.Timer;

/**
 * Created by lylw on 2017/8/2.
 */
public class TestClient1 {
    private static Long timeInterval = 180000L;
    private static Long delay_time = 0L;
    private static boolean _switch;
    private static Long startTime ;
    private static Timer timer;
    private static boolean timerTaskRunning=false;

    public static boolean isTimerTaskRunning() {
        return timerTaskRunning;
    }

    public static void setTimerTaskRunning(boolean timerTaskRunning) {
        TestClient1.timerTaskRunning = timerTaskRunning;
    }

    public static Long getTimeInterval() {
        return timeInterval;
    }

    public static void setTimeInterval(Long timeInterval) {
        TestClient1.timeInterval = timeInterval;
    }

    public static Long getDelay_time() {
        return delay_time;
    }

    public static void setDelay_time(Long delay_time) {
        TestClient1.delay_time = delay_time;
    }

    public static boolean is_switch() {
        return _switch;
    }

    public static void set_switch(boolean _switch) {
        TestClient1._switch = _switch;
    }

    public static Long getStartTime() {
        return startTime;
    }

    public static void setStartTime(Long startTime) {
        TestClient1.startTime = startTime;
    }

    public static void timerStart(Long delay_time, Long timeInterval){
        timer.schedule(new TransferThread(),delay_time,timeInterval);
    }

    public static void main(String[] args) throws IOException {
        //读取配置文件，设置配置项
        //传送文件
//        Scanner in = new Scanner(System.in);
        timer=new Timer();
        startTime=System.currentTimeMillis()+delay_time;
        timer.schedule(new TestTransferThread(),delay_time,timeInterval);
    }
}
