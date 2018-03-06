package cn.edu.tsinghua.iotdb.auth.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * This class contains all information of a User.
 */
public class User {
    public String name;
    public String password;
    public List<PathPrivilege> privilegeList;
    public List<String> roleList;
    public byte type; // 0 : admin, 1 : normal

    public User() {
    }

    public User(String name, String password) {
        this.name = name;
        this.password = password;
        this.privilegeList = new ArrayList<>();
        this.roleList = new ArrayList<>();
        this.type = 0;
    }

    public boolean hasPrivilege(PathPrivilege pathPrivilege) {
        return privilegeList.contains(pathPrivilege);
    }

    public boolean hasRole(String roleName) {
        return roleList.contains(roleName);
    }

    public boolean isAdmin() {
        return type == 0;
    }
}
