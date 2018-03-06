package cn.edu.tsinghua.iotdb.auth.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * This class contains all information of a role.
 */
public class Role {
    public String name;
    public List<PathPrivilege> privilegeList;

    public Role(String name) {
        this.name = name;
        this.privilegeList = new ArrayList<>();
    }
}
