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

    public boolean hasPrivilege(String path, int privilegeId) {
        for(PathPrivilege pathPrivilege : privilegeList) {
            if(pathPrivilege.path.equals(path) && pathPrivilege.privileges.contains(privilegeId))
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
}
