package cn.edu.tsinghua.iotdb.utils;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.entity.PrivilegeType;

public class ValidateUtils {
    private static final int MIN_PASSWORD_LENGTH = 4;
    private static final int MIN_USERNAME_LENGTH = 4;
    private static final int MIN_ROLENAME_LENGTH = 4;
    private static final String ROOT_PREFIX = "root";

    public static void validatePassword(String password) throws AuthException {
        if(password.length() < MIN_PASSWORD_LENGTH)
            throw new AuthException("Password's length must be greater than or equal to " + MIN_USERNAME_LENGTH);
    }

    public static void validateUsername(String username) throws AuthException {
        if(username.length() < MIN_USERNAME_LENGTH)
            throw new AuthException("Username's length must be greater than or equal to " + MIN_USERNAME_LENGTH);
    }

    public static void validateRolename(String rolename) throws AuthException {
        if(rolename.length() < MIN_ROLENAME_LENGTH)
            throw new AuthException("Role name's length must be greater than or equal to " + MIN_ROLENAME_LENGTH);
    }

    public static void validatePrivilege(int privilegeId) throws AuthException {
        if (privilegeId < 0 || privilegeId >= PrivilegeType.values().length) {
            throw new AuthException(String.format("Invalid privilegeId %d", privilegeId));
        }
    }

    public static void validatePath(String path) throws AuthException {
        if(!path.startsWith(ROOT_PREFIX))
            throw new AuthException(String.format("Illegal path %s, path should start with \"%\"", path, ROOT_PREFIX));
    }

    public static void validatePrivilegeOnPath(String path, int privilegeId) throws AuthException {
        validatePrivilege(privilegeId);
        PrivilegeType type = PrivilegeType.values()[privilegeId];
        if(path != null) {
            validatePath(path);
            switch (type) {
                case READ:
                case CREATE:
                case DELETE:
                case INSERT:
                case UPDATE:
                    return;
                default:
                    throw new AuthException(String.format("Illegal privilege %s on path %s", type.toString(), path));
            }
        } else {
            switch (type) {
                default:
                    throw new AuthException(String.format("Privilege %s requires a path", type.toString()));
            }
        }



    }
}
