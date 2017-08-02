package cn.edu.thu.tsfiledb.transferfile.transfer.client;

import cn.edu.thu.tsfiledb.transferfile.transfer.configure.ClientConfigure;

import java.io.IOException;
import java.util.Scanner;
import java.util.Timer;

/**
 * Created by lylw on 2017/7/17.
 */
public class Client {
    private static Long timeInterval = 180000L;
    private static Long delay_time = 0L;
    private static boolean _switch;
    private static Long startTime;
    private static Timer timer;
    private static boolean timerTaskRunning=false;

    public static boolean isTimerTaskRunning() {
        return timerTaskRunning;
    }

    public static void setTimerTaskRunning(boolean timerTaskRunning) {
        Client.timerTaskRunning = timerTaskRunning;
    }

    public static Long getTimeInterval() {
        return timeInterval;
    }

    public static void setTimeInterval(Long timeInterval) {
        Client.timeInterval = timeInterval;
    }

    public static Long getDelay_time() {
        return delay_time;
    }

    public static void setDelay_time(Long delay_time) {
        Client.delay_time = delay_time;
    }

    public static boolean is_switch() {
        return _switch;
    }

    public static void set_switch(boolean _switch) {
        Client._switch = _switch;
    }

    public static Long getStartTime() {
        return startTime;
    }

    public static void setStartTime(Long startTime) {
        Client.startTime = startTime;
    }

    public static void timerStart(Long delay_time, Long timeInterval){
        timer.schedule(new TransferThread(),delay_time,timeInterval);
    }

    public static void main(String[] args) throws IOException {
        //读取配置文件，设置配置项
        ClientConfigure.loadProperties();
        //传送文件
        Scanner in = new Scanner(System.in);
        timer=new Timer();
        startTime=System.currentTimeMillis()+delay_time;
        timer.schedule(new TransferThread(),delay_time,timeInterval);
        //Thread thread1=new Thread(new TransferThread());
        //thread1.start();
        //等待用户输入，设置传送任务
        while (true) {
            try {
                System.out.print("input a command:\n");
                String cmd = in.nextLine();
                if (cmd.equals("set")) {
                    System.out.print("input delay time: ");
                    delay_time = in.nextLong();
                    startTime=System.currentTimeMillis()+delay_time;
                    System.out.print("input time interval: ");
                    timeInterval = in.nextLong();

                    timer.cancel();
                    timer.purge();
                    timer = new Timer();
                    timer.schedule(new TransferThread(), delay_time, timeInterval);
                } else if (cmd.equals("transfer now")) {
                    Thread thread = new Thread(new TransferThread());
                    thread.start();
                }
                else if(cmd.equals("switch")){
                    System.out.print("set timing task on(1) or off(0):");
                    int getbool=in.nextInt();
                    _switch=(getbool==0)?false:true;
                    if(_switch){
                        //触发timer的schedule
                        timer.cancel();
                        timer.purge();
                        Long nowtime=System.currentTimeMillis();
                        while(startTime<nowtime){
                            startTime+=timeInterval;
                        }

                        delay_time=startTime-System.currentTimeMillis();
                        timer=new Timer();
                        timer.schedule(new TransferThread(),delay_time,timeInterval);
                    }
                    else if(!_switch){
                        timer.cancel();
                        timer.purge();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}