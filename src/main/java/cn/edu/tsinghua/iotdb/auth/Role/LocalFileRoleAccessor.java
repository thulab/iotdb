package cn.edu.tsinghua.iotdb.auth.Role;

import cn.edu.tsinghua.iotdb.auth.entity.PathPrivilege;
import cn.edu.tsinghua.iotdb.auth.entity.PrivilegeType;
import cn.edu.tsinghua.iotdb.auth.entity.Role;
import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.utils.IOUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class store each role in a separate sequential file.
 * Role file schema :
 *      Int32 role name length
 *      Utf-8 role name bytes
 *      Int32 path privilege number n
 *          Int32 path[1] length
 *          Utf-8 path[1] bytes
 *          Int32 privilege[1]
 *          Int32 path[2] length
 *          Utf-8 path[2] bytes
 *          Int32 privilege[2]
 *          ...
 *          Int32 path[n] length
 *          Utf-8 path[n] bytes
 *          Int32 privilege[n]
 */
public class LocalFileRoleAccessor implements IRoleAccessor {

    private static final String TEMP_SUFFIX = ".temp";
    private static final String STRING_ENCODING = "utf-8";

    private String roleDirPath;

    /**
     * Reused buffer for primitive types encoding/decoding, which aim to reduce memory fragments.
     * Use ThreadLocal for thread safety.
     */
    private ThreadLocal<ByteBuffer> encodingBufferLocal = new ThreadLocal<>();
    private ThreadLocal<byte[]> strBufferLocal = new ThreadLocal<>();

    LocalFileRoleAccessor(String roleDirPath) {
        this.roleDirPath = roleDirPath;
    }

    @Override
    public Role loadRole(String rolename) throws IOException {
        File roleProfile = new File(roleDirPath + File.separator + rolename + TsFileDBConstant.PROFILE_SUFFIX);
        if(!roleProfile.exists() || !roleProfile.isFile()) {
            return null;
        }
        FileInputStream inputStream = new FileInputStream(roleProfile);
        try (DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(inputStream))) {
            Role role = new Role();
            role.name = IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal);

            int privilegeNum = dataInputStream.readInt();
            List<PathPrivilege> pathPrivilegeList = new ArrayList<>();
            for (int i = 0; i < privilegeNum; i++) {
                String path = IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal);
                PrivilegeType privilegeType = PrivilegeType.values()[dataInputStream.readInt()];
                pathPrivilegeList.add(new PathPrivilege(privilegeType, path));
            }
            role.privilegeList = pathPrivilegeList;

            return role;
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    @Override
    public void saveRole(Role role) throws IOException {
        File roleProfile = new File(roleDirPath + File.separator + role.name + TsFileDBConstant.PROFILE_SUFFIX + TEMP_SUFFIX);
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(roleProfile));
        try {
            IOUtils.writeString(outputStream, role.name, STRING_ENCODING, encodingBufferLocal);

            int privilegeNum = role.privilegeList.size();
            IOUtils.writeInt(outputStream, privilegeNum, encodingBufferLocal);
            for(int i = 0; i < privilegeNum; i++) {
                PathPrivilege pathPrivilege = role.privilegeList.get(i);
                IOUtils.writeString(outputStream, pathPrivilege.path, STRING_ENCODING, encodingBufferLocal);
                IOUtils.writeInt(outputStream, pathPrivilege.type.ordinal(), encodingBufferLocal);
            }

        } catch (Exception e) {
            throw new IOException(e.getMessage());
        } finally {
            outputStream.flush();
            outputStream.close();
        }
        File oldFile = new File(roleDirPath + File.separator + role.name + TsFileDBConstant.PROFILE_SUFFIX);
        oldFile.delete();
        if(!roleProfile.renameTo(oldFile)) {
            throw new IOException(String.format("Cannot replace old role file with new one, role : %s", role.name));
        }
    }

    @Override
    public boolean deleteRole(String rolename) throws IOException {
        File roleProfile = new File(roleDirPath + File.separator + rolename + TsFileDBConstant.PROFILE_SUFFIX);
        if(!roleProfile.exists())
            return false;
        if(!roleProfile.delete()) {
            throw new IOException(String.format("Cannot delete role file of %s", rolename));
        }
        return true;
    }

    @Override
    public List<String> listAllRoles() {
        File userDir = new File(roleDirPath);
        String[] names = userDir.list((dir, name) -> name.endsWith(TsFileDBConstant.PROFILE_SUFFIX));
        return Arrays.asList(names != null ? names : new String[0]);
    }
}
