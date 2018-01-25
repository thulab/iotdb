package cn.edu.tsinghua.iotdb.exception.handler;

import java.io.*;
import java.util.HashMap;

public class ExceptionHandler {
    private  HashMap<String,String> ErrInfo = new HashMap<>();

    public void loadInfo(String FilePath,String LanguageVersion) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(
                    FilePath), "UTF-8"));
            String tempString;
            while ((tempString = reader.readLine()) != null) {
                String[] tempRes = tempString.split(" && ");
                if(LanguageVersion=="EN"){
                    ErrInfo.put(tempRes[1],"[Error: "+tempRes[0]+"] "+tempRes[2]);
                }
                else if(LanguageVersion=="CN"){
                    ErrInfo.put(tempRes[1],"[Error: "+tempRes[0]+"] "+tempRes[3]);
                }
            }
            reader.close();
            System.out.println(ErrInfo);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {}
            }
        }
    }
    public void writeInfo(int ErrCode,String ErrEnum,String Msg_CN,String Msg_EN,String FilePath){
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(FilePath, true),"UTF-8"));
            out.write("\r\n"+ErrCode+" && "+ErrEnum+" && "+Msg_EN+" && "+Msg_CN);
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
    public String searchInfo(String Enum){
        String SearchResult = ErrInfo.get(Enum);
        return SearchResult;
    }
    public void convertToDocument(String FilePath, String DocPath){
        BufferedReader reader = null;
        BufferedWriter out = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(
                    FilePath), "UTF-8"));
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(DocPath, true),"UTF-8"));
            String tempString;
            while ((tempString = reader.readLine()) != null) {
                String[] tempRes = tempString.split(" && ");
                String ErrCode=tempRes[0];
                String ErrEnum=tempRes[1];
                String Msg_EN=tempRes[2];
                String Msg_CN=tempRes[3];
                out.write("[Error: "+ErrCode+"] "+" "+Msg_EN+" ("+Msg_CN+")\r\n");
            }
            reader.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
