package cn.edu.tsinghua.iotdb.auth.entity;

import java.util.Objects;

/**
 * This class represent a privilege on a specific path. If the privilege is path-free, the path will be null.
 */
public class PathPrivilege {
    public PrivilegeType type;
    public String path;

    public PathPrivilege(PrivilegeType type, String path) {
        this.type = type;
        this.path = path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PathPrivilege that = (PathPrivilege) o;
        return type == that.type &&
                Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, path);
    }
}
