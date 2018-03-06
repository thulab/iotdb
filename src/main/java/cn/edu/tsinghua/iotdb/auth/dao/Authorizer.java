package cn.edu.tsinghua.iotdb.auth.dao;

import java.util.Set;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.IAuthorizer;
import cn.edu.tsinghua.iotdb.auth.model.Role;
import cn.edu.tsinghua.iotdb.auth.model.User;

/**
 * @author liukun
 */
public class Authorizer implements IAuthorizer{

    private static AuthDaoWrap authDaoWrap = new AuthDaoWrap();

    public static final Authorizer instance = new Authorizer();

    @Override
    public synchronized boolean login(String username, String password) throws AuthException {

        boolean status = false;
        status = authDaoWrap.checkUser(username, password);
        if (status == false) {
            throw new AuthException("The username or the password is not correct");
        }
        return status;
    }

    @Override
    public boolean createUser(String username, String password) throws AuthException {
        boolean status = false;
        
        if (username == null || password == null || "".equals(username) || "".equals(password)) {
            throw new AuthException("Username or password can't be empty");
        }
        
        User user = new User(username, password);

        status = authDaoWrap.addUser(user);
        if (status == false) {
            throw new AuthException("The user is exist");
        }
        return status;
    }

    @Override
    public  boolean deleteUser(String username) throws AuthException {
        boolean status = false;
        status = authDaoWrap.deleteUser(username);
        if (status == false) {
            throw new AuthException("The user is not exist");
        }
        return status;
    }

    @Override
    public boolean grantPrivilegeToUser(String username, String nodeName, int permissionId) throws AuthException {
        boolean status = false;
        status = authDaoWrap.addUserPermission(username, nodeName, permissionId);
        return status;
    }

    @Override
    public boolean revokePrivilegeFromUser(String userName, String nodeName, int permissionId) throws AuthException {
        boolean status = false;
        status = authDaoWrap.deleteUserPermission(userName, nodeName, permissionId);
        return status;
    }

    @Override
    public boolean createRole(String roleName) throws AuthException {
        boolean status = false;
        Role role = new Role(roleName);
        status = authDaoWrap.addRole(role);
        if (status == false) {
            throw new AuthException("The role is exist");
        }
        return status;
    }

    @Override
    public boolean deleteRole(String roleName) throws AuthException {
        boolean status = false;
        status = authDaoWrap.deleteRole(roleName);
        if (status == false) {
            throw new AuthException("The role is not exist");
        }
        return status;
    }

    @Override
    public boolean grantPrivilegeToRole(String roleName, String nodeName, int permissionId) throws AuthException {
        boolean status = false;
        status = authDaoWrap.addRolePermission(roleName, nodeName, permissionId);
        return status;
    }

    @Override
    public boolean revokePrivilegeFromRole(String roleName, String nodeName, int permissionId) throws AuthException {
        boolean status = false;
        status = authDaoWrap.deleteRolePermission(roleName, nodeName, permissionId);
        return status;
    }

    @Override
    public boolean grantRoleToUser(String roleName, String username) throws AuthException {
        boolean status = false;
        status = authDaoWrap.addUserRoleRel(username, roleName);
        return status;
    }

    @Override
    public boolean revokeRoleFromUser(String roleName, String username) throws AuthException {
        boolean status = false;
        status = authDaoWrap.deleteUserRoleRel(username, roleName);
        return status;
    }

    @Override
    public Set<Integer> getPrivileges(String username, String nodeName) throws AuthException {
        Set<Integer> permissionSets = null;
        permissionSets = authDaoWrap.getAllUserPermissions(username, nodeName);
        return permissionSets;
    }

    @Override
    public boolean updateUserPassword(String username, String newPassword) throws AuthException {
        boolean status = false;
        status = authDaoWrap.updateUserPassword(username, newPassword);
        if (status == false) {
            throw new AuthException("The username or the password is not correct");
        }
        return status;
    }

    @Override
    public boolean checkUserPrivileges(String username, String nodeName, int permissionId) {
        boolean status = false;
        status = authDaoWrap.checkUserPermission(username, nodeName, permissionId);
        return status;
    }

    @Override
    public void reset(){
    	authDaoWrap.reset();
    }

}
