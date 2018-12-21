package cn.edu.tsinghua.iotdb.script;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBStartServerScriptTest {
	private final String[] output = {
			"````````````````````````",
			"Starting IoTDB",
			"````````````````````````"
		};
	private final String START_IOTDB_STR = "IoTDB has started.";

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
		String startCMD = dir+File.separator+"iotdb"+File.separator+"bin"+File.separator+"start-server.bat";
		ProcessBuilder startBuilder = new ProcessBuilder("cmd.exe", "/c", startCMD);
		startBuilder.redirectErrorStream(true);
        Process startProess = startBuilder.start();
        BufferedReader startReader = new BufferedReader(new InputStreamReader(startProess.getInputStream()));
        List<String> runtimeOuput = new ArrayList<>();
        String line;
        try {
            while (true) {
                line = startReader.readLine();
                if (line == null) { 
                	break; 
                }
                runtimeOuput.add(line);
                if(line.indexOf(START_IOTDB_STR) > 0){
                	break;
                }
            }
            for(int i = 0; i < output.length;i++){
            	assertEquals(output[i], runtimeOuput.get(i));
            }
		} finally {
			startReader.close();
			startProess.destroy();
			String stopCMD = dir+File.separator+"iotdb"+File.separator+"bin"+File.separator+"stop-server.bat";
			ProcessBuilder stopBuilder = new ProcessBuilder("cmd.exe", "/c", stopCMD);
			stopBuilder.redirectErrorStream(true);
			Process stopProess = stopBuilder.start();
			BufferedReader stopReader = new BufferedReader(new InputStreamReader(stopProess.getInputStream()));
			while (true) {
                line = stopReader.readLine();
                if (line == null) { 
                	break; 
                }
                System.out.println(line);
            }
			stopReader.close();
			stopProess.destroy();
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
