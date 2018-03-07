package cn.edu.tsinghua.iotdb.auth.user;

public class LocalFileUserManager extends BasicUserManager {
    public LocalFileUserManager(String userDirPath) {
        super(new LocalFileUserAccessor(userDirPath));
    }
}
