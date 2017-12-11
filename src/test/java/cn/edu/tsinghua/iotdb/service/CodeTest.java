package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.auth.dao.DBDao;

import java.sql.*;

public class CodeTest {
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String derbyEmbeddedDriver = "org.apache.derby.jdbc.EmbeddedDriver";
        String protocal = "jdbc:derby:";
        String DBLocalPath = "src/test/resources/data/derby-tsfile-db-tttest";
        String createOrNot = ";create=true";
        // TODO Auto-generated method stub
//        Class.forName("com.mysql.jdbc.Driver");
        Class.forName(derbyEmbeddedDriver);
        Connection conn = DriverManager.getConnection(protocal + DBLocalPath + createOrNot);
        Statement stmt = conn.createStatement();
        stmt.close();
        conn.close();
        conn = DriverManager.getConnection(protocal + DBLocalPath + createOrNot);
        stmt = conn.createStatement();
        stmt.close();
        conn.close();
        System.out.println("ok");
        Class.forName(derbyEmbeddedDriver);
        conn = DriverManager.getConnection(protocal + DBLocalPath + createOrNot);
        stmt = conn.createStatement();
        stmt.close();
        conn.close();
        conn = DriverManager.getConnection(protocal + DBLocalPath + createOrNot);
        stmt = conn.createStatement();
        stmt.close();
        conn.close();
        System.out.println("ok");
    }

}
