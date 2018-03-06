package cn.edu.tsinghua.iotdb.auth.entity;

import cn.edu.tsinghua.iotdb.utils.ValidateUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class contains all information of a role.
 */
public class Role {
    public String name;
    public List<PathPrivilege> privilegeList;

    public Role() {
    }

    public Role(String name) {
        this.name = name;
        this.privilegeList = new ArrayList<>();
    }

    public boolean hasPrivilege(PathPrivilege pathPrivilege) {
        return privilegeList.contains(pathPrivilege);
    }

    /**
     *
     * @param path The path on which the privileges take effect. If path-free privileges are desired, this should be null.
     * @return The privileges granted to the role.
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
