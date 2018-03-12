package cn.edu.tsinghua.iotdb.auth.entity;

import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.utils.ValidateUtils;

import java.util.*;

/**
 * This class contains all information of a User.
 */
public class User {
    public String name;
    public String password;
    public List<PathPrivilege> privilegeList;
    public List<String> roleList;
    /**
     * The latest time when the user is referenced. Reserved to provide session control or LRU mechanism in the future.
     */
    public long lastActiveTime;

    public User() {
    }

    public User(String name, String password) {
        this.name = name;
        this.password = password;
        this.privilegeList = new ArrayList<>();
        this.roleList = new ArrayList<>();
    }

    public boolean hasPrivilege(String path, int privilegeId) {
        for(PathPrivilege pathPrivilege : privilegeList) {
            if((pathPrivilege.path == path || pathPrivilege.path.equals(path)) && pathPrivilege.privileges.contains(privilegeId))
                return true;
        }
        return false;
    }

    public void addPrivilege(String path, int privilgeId) {
        for(PathPrivilege pathPrivilege : privilegeList) {
            if(pathPrivilege.path.equals(path)) {
                pathPrivilege.privileges.add(privilgeId);
                return;
            }
        }
        PathPrivilege pathPrivilege = new PathPrivilege(path);
        pathPrivilege.privileges.add(privilgeId);
        privilegeList.add(pathPrivilege);
    }

    public void removePrivilege(String path, int privilgeId) {
        PathPrivilege emptyPrivilege = null;
        for(PathPrivilege pathPrivilege : privilegeList) {
            if(pathPrivilege.path.equals(path)) {
                pathPrivilege.privileges.remove(privilgeId);
                if(pathPrivilege.privileges.size() == 0)
                    emptyPrivilege = pathPrivilege;
                break;
            }
        }
        if(emptyPrivilege != null)
            privilegeList.remove(emptyPrivilege);
    }

    public boolean hasRole(String roleName) {
        return roleList.contains(roleName);
    }

    /**
     * Notice: The result of this method DOES NOT contain the privileges of the roles that this user plays.
     * @param path The path on which the privileges take effect. If path-free privileges are desired, this should be null.
     * @return ONLY the privileges specifically granted to the user.
     */
    public Set<Integer> getPrivileges(String path) {
        if(privilegeList == null)
            return null;
        Set<Integer> privileges = new HashSet<>();
        for(PathPrivilege pathPrivilege : privilegeList) {
            if(path != null){
                if (pathPrivilege.path != null && ValidateUtils.pathBelongsTo(path, pathPrivilege.path)) {
                    privileges.addAll(pathPrivilege.privileges);
                }
            } else {
                if (pathPrivilege.path == null) {
                    privileges.addAll(pathPrivilege.privileges);
                }
            }
        }
        return privileges;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User) o;
        return lastActiveTime == user.lastActiveTime &&
                Objects.equals(name, user.name) &&
                Objects.equals(password, user.password) &&
                Objects.equals(privilegeList, user.privilegeList) &&
                Objects.equals(roleList, user.roleList);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, password, privilegeList, roleList, lastActiveTime);
    }
}
