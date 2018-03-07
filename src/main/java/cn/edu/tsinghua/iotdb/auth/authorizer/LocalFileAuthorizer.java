package cn.edu.tsinghua.iotdb.auth.authorizer;

import cn.edu.tsinghua.iotdb.auth.Role.LocalFileRoleManager;
import cn.edu.tsinghua.iotdb.auth.user.LocalFileUserManager;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;

import java.io.File;

public class LocalFileAuthorizer extends BasicAuthorizer {
    private static TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
    private LocalFileAuthorizer() {
        super(new LocalFileUserManager(config.dataDir + File.separator + "users" + File.separator),
                new LocalFileRoleManager(config.dataDir + File.separator + "roles" + File.separator));
    }

    private static class InstanceHolder {
        private static LocalFileAuthorizer instance = new LocalFileAuthorizer();
    }

    public static LocalFileAuthorizer getInstance() {
        return InstanceHolder.instance;
    }
}
