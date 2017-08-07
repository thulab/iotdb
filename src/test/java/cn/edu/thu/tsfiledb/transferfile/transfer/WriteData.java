package cn.edu.thu.tsfiledb.transferfile.transfer;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by lylw on 2017/7/29.
 */
public class WriteData {
    public static void main(String[] args) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        FileInputStream fis=null;
        BufferedReader br=null;
        try{
            Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");

            statement = connection.createStatement();
            fis=new FileInputStream("G:\\tsfiledb_test\\sqlInsert\\create.txt");
            br=new BufferedReader(new InputStreamReader(fis));
            String createSql=null;
            while((createSql=br.readLine())!=null){
                statement.execute(createSql);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Timer timer=new Timer();
        timer.schedule(new WriteToDB(),0,120000);
    }
}
class WriteToDB extends TimerTask{
    public void run(){
        try {
            System.out.println("Start Write");
            writeToDB();
            System.out.println("End Write");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void writeToDB() throws SQLException, IOException {
        Connection connection = null;
        Statement statement = null;
        FileInputStream fis=null;
        BufferedReader br=null;
        try {
            Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");

            statement = connection.createStatement();
            fis=new FileInputStream("G:\\tsfiledb_test\\sqlInsert\\testsql.txt");
            br=new BufferedReader(new InputStreamReader(fis));

            String insertsql=null;
            while((insertsql=br.readLine())!=null){
                for(int i=0;i<250000;i++){
                    statement.execute(insertsql.concat("("+System.currentTimeMillis()+","+i+")"));
                }
            }
            statement.execute("close");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(statement != null){
                statement.close();
            }
            if(connection != null){
                connection.close();
            }
            br.close();
            fis.close();
        }
    }
}