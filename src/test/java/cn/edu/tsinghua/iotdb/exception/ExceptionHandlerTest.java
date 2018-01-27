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
            out.write("2000 && CR_UNKNOWN_ERROR && Unknown MySQL error && 未知MySQL错误\r\n");
            out.write("2001 && CR_NO_PARAMETERS EXISTS && No parameters exist in the statement && 语句中无变量\r\n");
            out.write("2002 && CR_INVALID_PARAMETER NO && Invalid parameter number && 无效的变量\r\n");
            out.write("2003 && CR_CONN_HOST_ERROR && Can't connect to MySQL server on {}({}) && 无法连接到MySQL服务器\r\n");
            out.write("2061 && CR_AUTH_PLUGIN_ERR && Authentication plugin {} reported error: {} && 验证失败\r\n");
            out.write("2062 && CR_INSECURE_API_ERR && Insecure API function call: {} && 不安全的函数调用\r\n");
            out.write("2064 && CR_OUT_OF_MEMORY && MySQL client ran out of memory && MySQL客户端内存溢出\r\n");
            out.write("2130 && CR_NO_PREPARE_STMT && Statement not prepared && 语句未就绪\r\n");
            out.write("2022 && CR_CON_FAIL_ERR && Fail to connect. && 连接失败\r\n");
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

        assertEquals("[Error: 2002] Invalid parameter number",excpHandler.searchInfo("CR_INVALID_PARAMETER NO"));
        assertEquals("[Error: 2003] Can't connect to MySQL server on {}({})",excpHandler.searchInfo("CR_CONN_HOST_ERROR"));

        excpHandler.loadInfo("ErrMan.txt","CN");
        assertEquals("[Error: 2003] 无法连接到MySQL服务器",excpHandler.searchInfo("CR_CONN_HOST_ERROR"));
        assertEquals("[Error: 2061] 验证失败",excpHandler.searchInfo("CR_AUTH_PLUGIN_ERR"));
    }

}
