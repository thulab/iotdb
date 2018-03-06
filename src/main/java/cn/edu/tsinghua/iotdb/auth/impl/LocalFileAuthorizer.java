package cn.edu.tsinghua.iotdb.auth.impl;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.IAuthorizer;
import cn.edu.tsinghua.iotdb.auth.Role.IRoleManager;
import cn.edu.tsinghua.iotdb.auth.Role.LocalFileRoleManager;
import cn.edu.tsinghua.iotdb.auth.entity.Role;
import cn.edu.tsinghua.iotdb.auth.entity.User;
import cn.edu.tsinghua.iotdb.auth.user.IUserManager;
import cn.edu.tsinghua.iotdb.auth.user.LocalFileUserAccessor;
import cn.edu.tsinghua.iotdb.auth.user.LocalFileUserManager;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.utils.ValidateUtils;

import java.io.File;
import java.util.List;
import java.util.Set;

public class LocalFileAuthorizer implements IAuthorizer {

    private TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
    private String userDirPath;
    private String roleDirPath;
    private IUserManager userManager;
    private IRoleManager roleManager;

    public LocalFileAuthorizer() {
        init();
    }

    private void init() {
        userDirPath = config.dataDir + File.separator + "users" + File.separator;
        roleDirPath = config.dataDir + File.separator + "roles" + File.separator;
        new File(userDirPath).mkdirs();
        new File(roleDirPath).mkdirs();

        userManager = new LocalFileUserManager(userDirPath);
        roleManager = new LocalFileRoleManager(roleDirPath);
    }

    @Override
    public boolean login(String username, String password) throws AuthException {
        User user = userManager.getUser(username);
        return user != null && user.password.equals(ValidateUtils.encryptPassword(password));
    }

    @Override
    public boolean createUser(String username, String password) throws AuthException {
        return userManager.createUser(username, password);
    }

    @Override
    public boolean deleteUser(String username) throws AuthException {
        return userManager.deleteUser(username);
    }

    @Override
    public boolean grantPrivilegeToUser(String username, String path, int privilegeId) throws AuthException {
        return userManager.grantPrivilegeToUser(username, path, privilegeId);
    }

    @Override
    public boolean revokePrivilegeFromUser(String username, String path, int privilegeId) throws AuthException {
        return userManager.revokePrivilegeFromUser(username, path, privilegeId);
    }

    @Override
    public boolean createRole(String roleName) throws AuthException {
        return roleManager.createRole(roleName);
    }

    @Override
    public boolean deleteRole(String roleName) throws AuthException {
        boolean success = roleManager.deleteRole(roleName);
        if(!success)
            return false;
        else {
            List<String> users = userManager.listAllUsers();
            for(String user : users) {
                user = user.replace(LocalFileUserAccessor.USER_PROFILE_SUFFIX,"");
                revokeRoleFromUser(roleName, user);
            }
        }
        return true;
    }

    @Override
    public boolean grantPrivilegeToRole(String roleName, String path, int privilegeId) throws AuthException {
        return roleManager.grantPrivilegeToRole(roleName, path, privilegeId);
    }

    @Override
    public boolean revokePrivilegeFromRole(String roleName, String path, int privilegeId) throws AuthException {
        return roleManager.revokePrivilegeFromRole(roleName, path, privilegeId);
    }

    @Override
    public boolean grantRoleToUser(String roleName, String username) throws AuthException {
        return userManager.grantRoleToUser(roleName, username);
    }

    @Override
    public boolean revokeRoleFromUser(String roleName, String username) throws AuthException {
        return userManager.revokeRoleFromUser(roleName, username);
    }

    @Override
    public Set<Integer> getPrivileges(String username, String path) throws AuthException {
        User user = userManager.getUser(username);
        if(user == null) {
            throw new AuthException("No such user");
        }
        Set<Integer> privileges = user.getPrivileges(path);
        for(String roleName : user.roleList) {
            Role role = roleManager.getRole(roleName);
            if(role != null) {
                privileges.addAll(role.getPrivileges(path));
            }
        }
        return privileges;
    }

    @Override
    public boolean updateUserPassword(String username, String newPassword) throws AuthException {
        return userManager.updateUserPassword(username, newPassword);
    }

    @Override
    public boolean checkUserPrivileges(String username, String path, int privilegeId) throws AuthException {
        if(LocalFileUserManager.ADMIN_NAME.equals(username))
            return true;
        return getPrivileges(username, path).contains(privilegeId);
    }

    @Override
    public void reset() {
        roleManager.reset();
        userManager.reset();
    }
}
