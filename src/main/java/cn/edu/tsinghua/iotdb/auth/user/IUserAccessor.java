package cn.edu.tsinghua.iotdb.auth.user;

import cn.edu.tsinghua.iotdb.auth.entity.User;

import java.io.IOException;

/**
 * This interface manages the serialization/deserialization of the user objects.
 */
public interface IUserAccessor {

    /**
     * Deserialize a user from its user file.
     * @param username The name of the user to be deserialized.
     * @return The user object or null if no such user.
     * @throws IOException
     */
    User loadUser(String username) throws IOException;

    /**
     * Serialize the user object to a temp file, then replace the old user file with the new file.
     * @param user The user object that is to be saved.
     * @throws IOException
     */
    void saveUser(User user) throws IOException;

    /**
     * Delete a user's user file.
     * @param username The name of the user to be deleted.
     * @return True if the file is successfully deleted, false if the file does not exists.
     * @throws IOException when the file cannot be deleted.
     */
    boolean deleteUser(String username) throws IOException;
}
