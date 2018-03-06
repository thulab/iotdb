package cn.edu.tsinghua.iotdb.auth.Role;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.HashLock;
import cn.edu.tsinghua.iotdb.auth.entity.PathPrivilege;
import cn.edu.tsinghua.iotdb.auth.entity.PrivilegeType;
import cn.edu.tsinghua.iotdb.auth.entity.Role;
import cn.edu.tsinghua.iotdb.utils.ValidateUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class read roles from local files through LocalFileRoleAccessor and manages them in a hash map.
 */
public class LocalFileRoleManager implements IRoleManager {

    private String roleDirath;
    private Map<String, Role> roleMap;
    private IRoleAccessor accessor;
    private HashLock lock;

    public LocalFileRoleManager(String roleDirath) {
        this.roleDirath = roleDirath;
        this.roleMap = new HashMap<>();
        this.accessor = new LocalFileRoleAccessor(roleDirath);
        this.lock = new HashLock();
    }

    @Override
    public Role getRole(String rolename) throws AuthException {
        lock.readLock(rolename);
        Role role = roleMap.get(rolename);
        try {
            if(role == null) {
                role = accessor.loadRole(rolename);
                if(role != null)
                    roleMap.put(rolename, role);
            }
        } catch (IOException e) {
            throw new AuthException(e);
        } finally {
            lock.readUnlock(rolename);
        }
        return role;
    }

    @Override
    public boolean createRole(String rolename) throws AuthException {
        ValidateUtils.validateRolename(rolename);

        Role role = getRole(rolename);
        if(role != null)
            return false;
        lock.writeLock(rolename);
        try {
            role = new Role(rolename);
            accessor.saveRole(role);
            roleMap.put(rolename, role);
            return true;
        } catch (IOException e) {
            throw new AuthException(e);
        } finally {
            lock.writeUnlock(rolename);
        }
    }

    @Override
    public boolean deleteRole(String rolename) throws AuthException {
        lock.writeLock(rolename);
        try {
            if(accessor.deleteRole(rolename)) {
                roleMap.remove(rolename);
                return true;
            } else
                return false;
        } catch (IOException e) {
            throw new AuthException(e);
        } finally {
            lock.writeUnlock(rolename);
        }
    }

    @Override
    public boolean grantPrivilegeToRole(String rolename, String path, int privilegeId) throws AuthException {
        ValidateUtils.validatePrivilegeOnPath(path, privilegeId);
        lock.writeLock(rolename);
        try {
            Role role = getRole(rolename);
            if(role == null) {
                throw new AuthException(String.format("No such role %s", rolename));
            }
            PathPrivilege pathPrivilege = new PathPrivilege(PrivilegeType.values()[privilegeId], path);
            if(role.hasPrivilege(pathPrivilege)) {
                return false;
            }
            role.privilegeList.add(pathPrivilege);
            try {
                accessor.saveRole(role);
            } catch (IOException e) {
                role.privilegeList.remove(pathPrivilege);
                throw new AuthException(e);
            }
            return true;
        } finally {
            lock.writeUnlock(rolename);
        }
    }

    @Override
    public boolean revokePrivilegeFromRole(String rolename, String path, int privilegeId) throws AuthException {
        ValidateUtils.validatePrivilegeOnPath(path, privilegeId);
        lock.writeLock(rolename);
        try {
            Role role = getRole(rolename);
            if(role == null) {
                throw new AuthException(String.format("No such role %s", rolename));
            }
            PathPrivilege pathPrivilege = new PathPrivilege(PrivilegeType.values()[privilegeId], path);
            if(!role.hasPrivilege(pathPrivilege)) {
                return false;
            }
            role.privilegeList.remove(pathPrivilege);
            try {
                accessor.saveRole(role);
            } catch (IOException e) {
                role.privilegeList.add(pathPrivilege);
                throw new AuthException(e);
            }
            return true;
        } finally {
            lock.writeUnlock(rolename);
        }
    }

    @Override
    public void reset() {
        roleMap.clear();
        lock.reset();
    }

    @Override
    public List<String> listAllRoles() {
        return accessor.listAllRoles();
    }
}
