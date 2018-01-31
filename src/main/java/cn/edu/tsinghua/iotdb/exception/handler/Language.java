package cn.edu.tsinghua.iotdb.exception.handler;

public enum Language {
    CN(2),
    EN(1);
    private int index;

    Language(int index){
        this.index = index;
    }

    public int getIndex(){
        return index;
    }

    public static boolean isSupported(String intput){
        try {
            Language language = Language.valueOf(intput);
            return true;
        } catch (Exception e){
            return false;
        }

    }
}
