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
		if (os.startsWith("windows")) {
			testStartClientOnWindows(".bat");
		} else {
			testStartClientOnUnix(".sh");
		}
	}
	
	private void testStartClientOnWindows(String suffix) throws IOException{
		final String[] output = {
				"````````````````````````",
				"Starting IoTDB",
				"````````````````````````"
			};
		String dir = getCurrentPath("cmd.exe", "/c", "echo %cd%");
		String startCMD = dir+File.separator+"iotdb"+File.separator+"bin"+File.separator+"start-server"+suffix;
		ProcessBuilder startBuilder = new ProcessBuilder("cmd.exe", "/c", startCMD);
		String stopCMD = dir+File.separator+"iotdb"+File.separator+"bin"+File.separator+"stop-server"+ suffix;
		ProcessBuilder stopBuilder = new ProcessBuilder("cmd.exe", "/c", stopCMD);
		testOutput(dir, suffix, startBuilder, stopBuilder, output);
	}
	
	private void testStartClientOnUnix(String suffix) throws IOException{
		String dir = getCurrentPath("pwd");
		final String[] output = { 
			"---------------------",
			"Starting IoTDB",
			"---------------------"
		};
		String startCMD = dir+File.separator+"iotdb"+File.separator+"bin"+File.separator+"start-server"+suffix;
		ProcessBuilder startBuilder = new ProcessBuilder("sh", startCMD);
		String stopCMD = dir+File.separator+"iotdb"+File.separator+"bin"+File.separator+"stop-server"+ suffix;
		ProcessBuilder stopBuilder = new ProcessBuilder("sh", stopCMD);
		testOutput(dir, suffix, startBuilder, stopBuilder, output);
	}
	
	private void testOutput(String dir, String suffix, ProcessBuilder startBuilder,  ProcessBuilder stopBuilder, String[] output) throws IOException {
		startBuilder.redirectErrorStream(true);
        Process startProcess = startBuilder.start();
        BufferedReader startReader = new BufferedReader(new InputStreamReader(startProcess.getInputStream()));
        List<String> runtimeOuput = new ArrayList<>();
        String line;
        try {
            while (true) {
                line = startReader.readLine();
                System.out.println(line);
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
			startProcess.destroy();
			stopBuilder.redirectErrorStream(true);
			Process stopProcess = stopBuilder.start();
			BufferedReader stopReader = new BufferedReader(new InputStreamReader(stopProcess.getInputStream()));
			while (true) {
                line = stopReader.readLine();
                if (line == null) { 
                	break; 
                }
                System.out.println(line);
            }
			stopReader.close();
//			stopProcess.destroy();
		}
	}
	
	private String getCurrentPath(String...command) throws IOException {
		ProcessBuilder builder = new ProcessBuilder(command);
		builder.redirectErrorStream(true);
		Process p = builder.start();
		BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String path = r.readLine();
		return path;
	}
}
