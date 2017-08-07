package cn.edu.thu.tsfiledb.writedata;

import java.util.Timer;

/**
 * Created by lylw on 2017/8/7.
 */
public class WriteDB {
    public static void main(String[] args){
        try {
            Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Timer timer=new Timer();
        timer.schedule(new SyntheticDataGenerator("d1",3000000,10),0,600000);
    }
}
