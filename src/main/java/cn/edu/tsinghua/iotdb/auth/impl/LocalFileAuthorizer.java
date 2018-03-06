package cn.edu.tsinghua.iotdb.auth.impl;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.IAuthorizer;

import java.util.Set;

public class LocalFileAuthorizer implements IAuthorizer {
    @Override
    public boolean login(String username, String password) throws AuthException {
        return false;
    }

    @Override
    public boolean createUser(String username, String password) throws AuthException {
        return false;
    }

    @Override
    public boolean deleteUser(String username) throws AuthException {
        return false;
    }

    @Override
    public boolean grantPrivilegeToUser(String username, String path, int privilegeId) throws AuthException {
        return false;
    }

    @Override
    public boolean revokePrivilegeFromUser(String username, String path, int privilegeId) throws AuthException {
        return false;
    }

    @Override
    public boolean createRole(String roleName) throws AuthException {
        return false;
    }

    @Override
    public boolean deleteRole(String roleName) throws AuthException {
        return false;
    }

    @Override
    public boolean grantPrivilegeToRole(String roleName, String path, int privilegeId) throws AuthException {
        return false;
    }

    @Override
    public boolean revokePrivilegeFromRole(String roleName, String path, int privilegeId) throws AuthException {
        return false;
    }

    @Override
    public boolean grantRoleToUser(String roleName, String username) throws AuthException {
        return false;
    }

    @Override
    public boolean revokeRoleFromUser(String roleName, String username) throws AuthException {
        return false;
    }

    @Override
    public Set<Integer> getPermission(String username, String path) throws AuthException {
        return null;
    }

    @Override
    public boolean updateUserPassword(String username, String newPassword) throws AuthException {
        return false;
    }

    @Override
    public boolean checkUserPermission(String username, String path, int privilegeId) throws AuthException {
        return false;
    }

    @Override
    public void reset() {

    }
}
