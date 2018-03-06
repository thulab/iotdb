package cn.edu.tsinghua.iotdb.auth.user;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.HashLock;
import cn.edu.tsinghua.iotdb.auth.entity.PathPrivilege;
import cn.edu.tsinghua.iotdb.auth.entity.PrivilegeType;
import cn.edu.tsinghua.iotdb.auth.entity.User;
import cn.edu.tsinghua.iotdb.utils.ValidateUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class stores information of each user in a separate file within a directory, and cache them in memory when a user is accessed.
 */
public class LocalFileUserManager implements IUserManager {

    private String userDir;
    private Map<String, User> userMap;
    private IUserAccessor accessor;
    private HashLock lock;

    public LocalFileUserManager(String userDir) {
        this.userDir = userDir;
        this.userMap = new HashMap<>();
        this.accessor = new LocalFileUserAccessor(userDir);
        this.lock = new HashLock();
    }

    @Override
    public User getUser(String username) throws AuthException {
        lock.readLock(username);
        User user = userMap.get(username);
        try {
            if(user == null) {
                user = accessor.loadUser(username);
                if(user != null)
                    userMap.put(username, user);
            }
        } catch (IOException e) {
            throw new AuthException(e);
        } finally {
            lock.readUnlock(username);
        }
        return user;
    }

    @Override
    public boolean createUser(String username, String password) throws AuthException {
        ValidateUtils.validateUsername(username);
        ValidateUtils.validatePassword(password);

        User user = getUser(username);
        if(user != null)
            return false;
        lock.writeLock(username);
        try {
            user = new User(username, password);
            accessor.saveUser(user);
            userMap.put(username, user);
            return true;
        } catch (IOException e) {
            throw new AuthException(e);
        } finally {
            lock.writeUnlock(username);
        }
    }

    @Override
    public boolean deleteUser(String username) throws AuthException {
        lock.writeLock(username);
        try {
            if(accessor.deleteUser(username)) {
                userMap.remove(username);
                return true;
            } else
                return false;
        } catch (IOException e) {
            throw new AuthException(e);
        } finally {
            lock.writeUnlock(username);
        }
    }

    @Override
    public boolean grantPrivilegeToUser(String username, String path, int privilegeId) throws AuthException {
        ValidateUtils.validatePrivilegeOnPath(path, privilegeId);
        lock.writeLock(username);
        try {
            User user = getUser(username);
            if(user == null) {
                throw new AuthException(String.format("No such user %s", username));
            }
            PathPrivilege pathPrivilege = new PathPrivilege(PrivilegeType.values()[privilegeId], path);
            if(user.hasPrivilege(pathPrivilege)) {
                return false;
            }
            user.privilegeList.add(pathPrivilege);
            try {
                accessor.saveUser(user);
            } catch (IOException e) {
                user.privilegeList.remove(pathPrivilege);
                throw new AuthException(e);
            }
            return true;
        } finally {
            lock.writeUnlock(username);
        }
    }

    @Override
    public boolean revokePrivilegeFromUser(String username, String path, int privilegeId) throws AuthException {
        ValidateUtils.validatePrivilegeOnPath(path, privilegeId);
        lock.writeLock(username);
        try {
            User user = getUser(username);
            if(user == null) {
                throw new AuthException(String.format("No such user %s", username));
            }
            PathPrivilege pathPrivilege = new PathPrivilege(PrivilegeType.values()[privilegeId], path);
            if(!user.hasPrivilege(pathPrivilege)) {
                return false;
            }
            user.privilegeList.remove(pathPrivilege);
            try {
                accessor.saveUser(user);
            } catch (IOException e) {
                user.privilegeList.add(pathPrivilege);
                throw new AuthException(e);
            }
            return true;
        } finally {
            lock.writeUnlock(username);
        }
    }

    @Override
    public boolean updateUserPassword(String username, String newPassword) throws AuthException {
        try {
            ValidateUtils.validatePassword(newPassword);
        } catch (AuthException e) {
            return false;
        }
        
        lock.writeLock(username);
        try {
            User user = getUser(username);
            if(user == null) {
                throw new AuthException(String.format("No such user %s", username));
            }
            String oldPassword = user.password;
            user.password = newPassword;
            try {
                accessor.saveUser(user);
            } catch (IOException e) {
                user.password = oldPassword;
                throw new AuthException(e);
            }
            return true;
        } finally {
            lock.writeUnlock(username);
        }
    }

}
