package cn.edu.tsinghua.iotdb.exception.handler;

import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class ExceptionHandler {
    private  HashMap<Integer,String> errInfo = new HashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(TsfileDBDescriptor.class);
    public static final String CONFIG_NAME = "error_info.txt";
    private static final String encodeMode = "UTF-8";
    private static final String separator = " && ";

    private static final ExceptionHandler INSTANCE = new ExceptionHandler();
    public static final ExceptionHandler getInstance() {
        return ExceptionHandler.INSTANCE;
    }

    public void loadInfo(String filePath, String languageVersion){
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(
                    filePath), encodeMode));
            String tempString;
            while ((tempString = reader.readLine()) != null) {
                String[] tempRes = tempString.split(separator);
                Language language = Language.valueOf(languageVersion);
                int index = language.getIndex();
                errInfo.put(Integer.parseInt(tempRes[0]),"[Error: "+tempRes[index]+"]");
            }
            reader.close();
        } catch (IOException e) {
            LOGGER.error("Read file error. File does not exist or file is broken. File path: {}.Because: {}.",filePath,e.getMessage());
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOGGER.error("Fail to close file: {}. Because: {}.",filePath,e.getMessage());
                }
            }
        }
    }

    public void loadInfo(){
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
        Language language = Language.valueOf(TsfileDBDescriptor.getInstance().getConfig().languageVersion);
        loadInfo(url,language.name());
    }
    public void writeInfo(int errCode,String errEnum,String msg_CN,String msg_EN,String filePath){
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(filePath, true),encodeMode));
            out.write("\r\n"+errCode+separator+errEnum+separator+msg_EN+separator+msg_CN);
        } catch (Exception e) {
            LOGGER.error("Fail to open file: {}. Because: {}.",filePath,e.getMessage());
        } finally {
            try {
                if(out!=null)
                    out.close();
            } catch (IOException e) {
                LOGGER.error("Fail to close file: {}. Because: {}.",filePath,e.getMessage());
            }
        }
    }
    public String searchInfo(int errCode){
        return errInfo.get(errCode);
    }
    
    public void convertToDocument(String filePath, String docPath){
        BufferedReader reader = null;
        BufferedWriter out = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(
                    filePath), encodeMode));
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(docPath, true),encodeMode));
            String tempString;
            StringBuilder builder = new StringBuilder();

            while ((tempString = reader.readLine()) != null) {
                String[] tempRes = tempString.split(separator);
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
            LOGGER.error("Fail to convert file to document. File path: {}, document path: {}. Because: {}.", filePath, docPath,e.getMessage());
        } finally {
            try {
                if(reader != null){
                    reader.close();
                }
            }catch(IOException e) {
                LOGGER.error("Fail to close file: {}. Because: {}.",filePath,e.getMessage());
            }
            try{
                if(out != null){
                    out.close();
                }
            } catch (IOException e) {
                LOGGER.error("Fail to close document: {}. Because: {}.", docPath,e.getMessage());
            }
        }
    }
}
