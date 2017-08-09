package cn.edu.thu.tsfiledb.transferfile.transfer.writedata;

import java.sql.SQLException;

/**
 * Created by lylw on 2017/8/7.
 */
public class WriteDB {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        try {
            Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        (new SyntheticDataGenerator("d1",1000000,1,true)).start(10000);
//        Timer timer=new Timer();
//        timer.schedule(new SyntheticDataGenerator("d1",10000,1,false),0,60000);
    }
}
