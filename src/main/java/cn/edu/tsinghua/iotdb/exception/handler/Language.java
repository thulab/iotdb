package cn.edu.tsinghua.iotdb.exception.handler;

public enum Language {
    CN(3),
    EN(2);
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
