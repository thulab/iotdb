package cn.edu.tsinghua.iotdb.auth.authorizer;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.Role.IRoleManager;
import cn.edu.tsinghua.iotdb.auth.entity.Role;
import cn.edu.tsinghua.iotdb.auth.entity.User;
import cn.edu.tsinghua.iotdb.auth.user.IUserManager;
import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.exception.StartupException;
import cn.edu.tsinghua.iotdb.service.IService;
import cn.edu.tsinghua.iotdb.service.ServiceType;
import cn.edu.tsinghua.iotdb.utils.ValidateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

abstract public class BasicAuthorizer implements IAuthorizer,IService {

    private static final Logger logger = LoggerFactory.getLogger(BasicAuthorizer.class);

    private IUserManager userManager;
    private IRoleManager roleManager;

    BasicAuthorizer(IUserManager userManager, IRoleManager roleManager) {
        this.userManager = userManager;
        this.roleManager = roleManager;
        init();
    }

    protected void init() {
        userManager.reset();
        roleManager.reset();
        logger.info("Initialization of Authorizer completes");
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
        if(TsFileDBConstant.ADMIN_NAME.equals(username))
            throw new AuthException("Default administrator cannot be deleted");
        return userManager.deleteUser(username);
    }

    @Override
    public boolean grantPrivilegeToUser(String username, String path, int privilegeId) throws AuthException {
        if(TsFileDBConstant.ADMIN_NAME.equals(username))
            throw new AuthException("Invalid operation, administrator already has all privileges");
        return userManager.grantPrivilegeToUser(username, path, privilegeId);
    }

    @Override
    public boolean revokePrivilegeFromUser(String username, String path, int privilegeId) throws AuthException {
        if(TsFileDBConstant.ADMIN_NAME.equals(username))
            throw new AuthException("Invalid operation, administrator must have all privileges");
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
            // proceed to revoke the role in all users
            List<String> users = userManager.listAllUsers();
            for(String user : users) {
                try {
                    userManager.revokeRoleFromUser(roleName, user);
                } catch (AuthException e) {
                    logger.warn("Error encountered when revoking a role {} from user {} after deletion, because {}", roleName, user, e);
                }
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
        Role role = roleManager.getRole(roleName);
        if(role == null) {
            throw new AuthException(String.format("No such role : %s", roleName));
        }
        return userManager.grantRoleToUser(roleName, username);
    }

    @Override
    public boolean revokeRoleFromUser(String roleName, String username) throws AuthException {
        Role role = roleManager.getRole(roleName);
        if(role == null) {
            throw new AuthException(String.format("No such role : %s", roleName));
        }
        return userManager.revokeRoleFromUser(roleName, username);
    }

    @Override
    public Set<Integer> getPrivileges(String username, String path) throws AuthException {
        User user = userManager.getUser(username);
        if(user == null) {
            throw new AuthException("No such user");
        }
        // get privileges of the user
        Set<Integer> privileges = user.getPrivileges(path);
        // merge the privileges of the roles of the user
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
        return TsFileDBConstant.ADMIN_NAME.equals(username) || getPrivileges(username, path).contains(privilegeId);
    }

    @Override
    public void reset() {
        init();
    }

    @Override
    public void start() throws StartupException {
        init();
    }

    @Override
    public void stop() {

    }

    @Override
    public ServiceType getID() {
        return ServiceType.AUTHORIZATION_SERVICE;
    }
}