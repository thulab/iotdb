package cn.edu.tsinghua.iotdb.auth.Role;

import cn.edu.tsinghua.iotdb.auth.entity.Role;

import java.io.IOException;

/**
 * This interface manages the serialization/deserialization of the role objects.
 */
public interface IRoleAccessor {
    /**
     * Deserialize a role from its role file.
     * @param rolename The name of the role to be deserialized.
     * @return The role object or null if no such role.
     * @throws IOException
     */
    Role loadRole(String rolename) throws IOException;

    /**
     * Serialize the role object to a temp file, then replace the old role file with the new file.
     * @param role The role object that is to be saved.
     * @throws IOException
     */
    void saveRole(Role role) throws IOException;

    /**
     * Delete a role's role file.
     * @param rolename The name of the role to be deleted.
     * @return True if the file is successfully deleted, false if the file does not exists.
     * @throws IOException when the file cannot be deleted.
     */
    boolean deleteRole(String rolename) throws IOException;
}
