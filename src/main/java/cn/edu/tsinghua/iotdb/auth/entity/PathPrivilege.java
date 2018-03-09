package cn.edu.tsinghua.iotdb.auth.entity;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * This class represent a privilege on a specific path. If the privilege is path-free, the path will be null.
 */
public class PathPrivilege {
    public Set<Integer> privileges;
    public String path;

    public PathPrivilege(String path) {
        this.path = path;
        this.privileges = new HashSet<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PathPrivilege that = (PathPrivilege) o;
        return Objects.equals(privileges, that.privileges) &&
                Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {

        return Objects.hash(privileges, path);
    }
}
