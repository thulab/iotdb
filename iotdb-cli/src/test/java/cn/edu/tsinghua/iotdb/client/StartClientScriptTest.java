package cn.edu.tsinghua.iotdb.client;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StartClientScriptTest {
	private final String[] output = {
			"````````````````````````",
			"Starting IoTDB Client",
			"````````````````````````",
			"IoTDB> Connection Error, please check whether the network is avaliable or the server has started.. Host is 127.0.0.1, port is 6667."
		};

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws IOException, InterruptedException {
        String os = System.getProperty("os.name").toLowerCase();
        if(os.startsWith("windows")){
    		testStartClientOnWindows();
        } else {
        	testStartClientOnUnix();
        }
	}
	
	private void testStartClientOnWindows() throws IOException{
		String dir = getCurrentPathOnWinddows();
		ProcessBuilder builder = new ProcessBuilder("cmd.exe", 
				"/c", 
				dir+File.separator+"cli"+File.separator+"bin"+File.separator+"start-client.bat",
				"-h",
				"xxx.xxx.xxx.xxx",
				"-p",
				"xxxx",
				"-u",
				"root",
				"-pw",
				"root");
        builder.redirectErrorStream(true);
        Process p = builder.start();
        BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line;
        int i = 0;
        while (true) {
            line = r.readLine();
            if (line == null) { 
            	break; 
            }
            assertEquals(output[i], line);
            i++;
        }
	}
	
	private void testStartClientOnUnix(){
		
	}
	
	private String getCurrentPathOnWinddows() throws IOException{
		ProcessBuilder builder = new ProcessBuilder("cmd.exe", "/c", "echo %cd%");
	    builder.redirectErrorStream(true);
	    Process p = builder.start();
	    BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
	    String path = r.readLine();
	    return path;
	}

}
