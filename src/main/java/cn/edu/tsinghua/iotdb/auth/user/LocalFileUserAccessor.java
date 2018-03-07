package cn.edu.tsinghua.iotdb.auth.user;

import cn.edu.tsinghua.iotdb.auth.entity.PathPrivilege;
import cn.edu.tsinghua.iotdb.auth.entity.User;
import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.utils.IOUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class loads a user's information from the corresponding file.The user file is a sequential file.
 * User file schema:
 *      Int32 username bytes length
 *      Utf-8 username bytes
 *      Int32 Password bytes length
 *      Utf-8 password bytes
 *      Int32 path privilege number n
 *          Int32 path[1] length
 *          Utf-8 path[1] bytes
 *          Int32 privilege num k1
 *              Int32 privilege[1][1]
 *              Int32 privilege[1][2]
 *              ...
 *              Int32 privilege[1][k1]
 *          Int32 path[2] length
 *          Utf-8 path[2] bytes
 *          Int32 privilege num k2
 *              Int32 privilege[2][1]
 *              Int32 privilege[2][2]
 *              ...
 *              Int32 privilege[2][k2]
 *          ...
 *          Int32 path[n] length
 *          Utf-8 path[n] bytes
 *          Int32 privilege num kn
 *              Int32 privilege[n][1]
 *              Int32 privilege[n][2]
 *              ...
 *              Int32 privilege[n][kn]
 *      Int32 role name number m
 *          Int32 role name[1] length
 *          Utf-8 role name[1] bytes
 *          Int32 role name[2] length
 *          Utf-8 role name[2] bytes
 *          ...
 *          Int32 role name[m] length
 *          Utf-8 role name[m] bytes
 */
public class LocalFileUserAccessor implements IUserAccessor{
    private static final String TEMP_SUFFIX = ".temp";
    private static final String STRING_ENCODING = "utf-8";

    private String userDirPath;
    /**
     * Reused buffer for primitive types encoding/decoding, which aim to reduce memory fragments.
     * Use ThreadLocal for thread safety.
     */
    private ThreadLocal<ByteBuffer> encodingBufferLocal = new ThreadLocal<>();
    private ThreadLocal<byte[]> strBufferLocal = new ThreadLocal<>();

    LocalFileUserAccessor(String userDirPath) {
        this.userDirPath = userDirPath;
    }

    /**
     * Deserialize a user from its user file.
     * @param username The name of the user to be deserialized.
     * @return The user object or null if no such user.
     * @throws IOException
     */
    public User loadUser(String username) throws IOException{
        File userProfile = new File(userDirPath + File.separator + username + TsFileDBConstant.PROFILE_SUFFIX);
        if(!userProfile.exists() || !userProfile.isFile()) {
            return null;
        }
        FileInputStream inputStream = new FileInputStream(userProfile);
        try (DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(inputStream))) {
            User user = new User();
            user.name = IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal);
            user.password = IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal);

            int privilegeNum = dataInputStream.readInt();
            List<PathPrivilege> pathPrivilegeList = new ArrayList<>();
            for (int i = 0; i < privilegeNum; i++) {
                pathPrivilegeList.add(IOUtils.readPathPrivilege(dataInputStream, STRING_ENCODING, strBufferLocal));
            }
            user.privilegeList = pathPrivilegeList;

            int roleNum = dataInputStream.readInt();
            List<String> roleList = new ArrayList<>();
            for (int i = 0; i < roleNum; i++) {
                String roleName = IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal);
                roleList.add(roleName);
            }
            user.roleList = roleList;

            return user;
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    /**
     * Serialize the user object to a temp file, then replace the old user file with the new file.
     * @param user The user object that is to be saved.
     * @throws IOException
     */
    public void saveUser(User user) throws IOException{
        File userProfile = new File(userDirPath + File.separator + user.name + TsFileDBConstant.PROFILE_SUFFIX + TEMP_SUFFIX);
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(userProfile));
        try {
            IOUtils.writeString(outputStream, user.name, STRING_ENCODING, encodingBufferLocal);
            IOUtils.writeString(outputStream, user.password, STRING_ENCODING, encodingBufferLocal);

            int privilegeNum = user.privilegeList.size();
            IOUtils.writeInt(outputStream, privilegeNum, encodingBufferLocal);
            for(int i = 0; i < privilegeNum; i++) {
                PathPrivilege pathPrivilege = user.privilegeList.get(i);
                IOUtils.writePathPrivilege(outputStream, pathPrivilege, STRING_ENCODING, encodingBufferLocal);
            }

            int roleNum = user.roleList.size();
            IOUtils.writeInt(outputStream, roleNum, encodingBufferLocal);
            for(int i = 0; i < roleNum; i++) {
                IOUtils.writeString(outputStream, user.roleList.get(i), STRING_ENCODING, encodingBufferLocal);
            }

        } catch (Exception e) {
            throw new IOException(e.getMessage());
        } finally {
            outputStream.flush();
            outputStream.close();
        }

        File oldFile = new File(userDirPath + File.separator + user.name + TsFileDBConstant.PROFILE_SUFFIX);
        oldFile.delete();
        if(!userProfile.renameTo(oldFile)) {
            throw new IOException(String.format("Cannot replace old user file with new one, user : %s", user.name));
        }
    }

    /**
     * Delete a user's user file.
     * @param username The name of the user to be deleted.
     * @return True if the file is successfully deleted, false if the file does not exists.
     * @throws IOException when the file cannot be deleted.
     */
    public boolean deleteUser(String username) throws IOException{
        File userProfile = new File(userDirPath + File.separator + username + TsFileDBConstant.PROFILE_SUFFIX);
        if(!userProfile.exists())
            return false;
        if(!userProfile.delete()) {
            throw new IOException(String.format("Cannot delete user file of %s", username));
        }
        return true;
    }

    @Override
    public List<String> listAllUsers() {
        File userDir = new File(userDirPath);
        String[] names = userDir.list((dir, name) -> name.endsWith(TsFileDBConstant.PROFILE_SUFFIX));
        return Arrays.asList(names != null ? names : new String[0]);
    }

    @Override
    public void reset() {
        new File(userDirPath).mkdirs();
    }
}
