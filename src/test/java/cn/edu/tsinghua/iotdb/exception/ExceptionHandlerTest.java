package cn.edu.tsinghua.iotdb.exception;

import cn.edu.tsinghua.iotdb.exception.handler.ExceptionHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;

public class ExceptionHandlerTest {

    @Before
    public void before() {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream("ErrMan.txt", true),"UTF-8"));
            out.write("20000 && Unknown MySQL error && 未知MySQL错误\r\n");
            out.write("20001 && No parameters exist in the statement && 语句中无变量\r\n");
            out.write("20002 && Invalid parameter number && 无效的变量\r\n");
            out.write("20003 && Can't connect to MySQL server on {}({}) && 无法连接到MySQL服务器\r\n");
            out.write("20061 && Authentication plugin {} reported error: {} && 验证失败\r\n");
            out.write("20062 && Insecure API function call: {} && 不安全的函数调用\r\n");
            out.write("20064 && MySQL client ran out of memory && MySQL客户端内存溢出\r\n");
            out.write("20130 && Statement not prepared && 语句未就绪\r\n");
            out.write("20220 && Fail to connect. && 连接失败\r\n");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @After
    public void after() {
        File file = new File("ErrMan.txt");
        if(file.exists()){
            file.delete();
        }
    }

    @Test
    public void testLoadProp() {
        ExceptionHandler excpHandler = new ExceptionHandler();
        excpHandler.loadInfo("ErrMan.txt","EN");

        assertEquals("[Error: Invalid parameter number]",excpHandler.searchInfo(20002));
        assertEquals("[Error: Can't connect to MySQL server on {}({})]",excpHandler.searchInfo(20003));

        excpHandler.loadInfo("ErrMan.txt","CN");
        assertEquals("[Error: 无法连接到MySQL服务器]",excpHandler.searchInfo(20003));
        assertEquals("[Error: 验证失败]",excpHandler.searchInfo(20061));
    }

}
