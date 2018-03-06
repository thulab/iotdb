package cn.edu.tsinghua.iotdb.auth.entity;

import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.utils.ValidateUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class contains all information of a User.
 */
public class User {
    public String name;
    public String password;
    public List<PathPrivilege> privilegeList;
    public List<String> roleList;
    public long lastActiveTime;

    public User() {
    }

    public User(String name, String password) {
        this.name = name;
        this.password = password;
        this.privilegeList = new ArrayList<>();
        this.roleList = new ArrayList<>();
    }

    public boolean hasPrivilege(PathPrivilege pathPrivilege) {
        return privilegeList.contains(pathPrivilege);
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
                    privileges.add(pathPrivilege.type.ordinal());
                }
            } else {
                if (pathPrivilege.path == null) {
                    privileges.add(pathPrivilege.type.ordinal());
                }
            }
        }
        return privileges;
    }
}
