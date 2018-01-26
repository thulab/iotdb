package cn.edu.tsinghua.iotdb.exception.handler;

import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;

public class ExceptionHandler {
    private  HashMap<String,String> errInfo = new HashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(TsfileDBDescriptor.class);
    public static final String CONFIG_NAME = "error_info.txt";

    private static final ExceptionHandler INSTANCE = new ExceptionHandler();
    public static final ExceptionHandler getInstance() {
        return ExceptionHandler.INSTANCE;
    }

    public void loadInfo() {
        BufferedReader reader = null;
        String url = System.getProperty(TsFileDBConstant.IOTDB_CONF, null);
        if (url == null) {
            url = System.getProperty(TsFileDBConstant.IOTDB_HOME, null);
            if (url != null) {
                url = url + File.separatorChar + "conf" + File.separatorChar + ExceptionHandler.CONFIG_NAME;
            } else {
                LOGGER.warn("Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file {}, use default configuration", TsfileDBConfig.CONFIG_NAME);
                return;
            }
        } else{
            url += (File.separatorChar + ExceptionHandler.CONFIG_NAME);
        }

        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(
                    url), "UTF-8"));
            String tempString;
            while ((tempString = reader.readLine()) != null) {
                String[] tempRes = tempString.split(" && ");
                Language language = Language.valueOf(TsfileDBDescriptor.getInstance().getConfig().languageVersion);
                int index = language.getIndex();
                errInfo.put(tempRes[1],"[Error: "+tempRes[0]+"] "+tempRes[index]);
            }
            reader.close();
        } catch (IOException e) {
            LOGGER.error("Read file error. File does not exist or file is broken. File path: {}.",url);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOGGER.error("Fail to close file: {}.",url);
                }
            }
        }
    }
    public void writeInfo(int errCode,String errEnum,String msg_CN,String msg_EN,String filePath){
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(filePath, true),"UTF-8"));
            out.write("\r\n"+errCode+" && "+errEnum+" && "+msg_EN+" && "+msg_CN);
        } catch (Exception e) {
            LOGGER.error("Fail to open file: {}.",filePath);
        } finally {
            try {
                if(out!=null)
                    out.close();
            } catch (IOException e) {
                LOGGER.error("Fail to close file: {}.",filePath);
            }
        }
    }
    public String searchInfo(String errEnum){
        String SearchResult = errInfo.get(errEnum);
        return SearchResult;
    }
    public void convertToDocument(String filePath, String docPath){
        BufferedReader reader = null;
        BufferedWriter out = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(
                    filePath), "UTF-8"));
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(docPath, true),"UTF-8"));
            String tempString;
            StringBuilder builder = new StringBuilder();

            while ((tempString = reader.readLine()) != null) {
                String[] tempRes = tempString.split(" && ");
                //builder.append("[Error: "+tempRes[0]+"] "+tempRes[1]+" "+tempRes[2]+" ("+tempRes[3]+")\r\n");
                builder.append("[Error: "+tempRes[0]+"] "+tempRes[1]+" "+tempRes[2]+" (");
                for(int i=3;i<tempRes.length;i++){
                    if(i==tempRes.length-1){
                        builder.append(tempRes[i]);
                    }
                    else{
                        builder.append(tempRes[i]+", ");
                    }
                }
                builder.append(")\r\n");
            }
            out.write(builder.toString());
            reader.close();
            out.close();
        } catch (IOException e) {
            LOGGER.error("Fail to convert file to document. File path: {}, document path: {}", filePath, docPath);
        } finally {
            try {
                if(reader != null){
                    reader.close();
                }
            }catch(IOException e) {
                LOGGER.error("Fail to close file: {}",filePath);
            }
            try{
                if(out != null){
                    out.close();
                }
            } catch (IOException e) {
                LOGGER.error("Fail to close document: {}", docPath);
            }
        }
    }
}
