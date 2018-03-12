package cn.edu.tsinghua.iotdb.utils;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.entity.PrivilegeType;
import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ValidateUtils {
    private static final int MIN_PASSWORD_LENGTH = 4;
    private static final int MIN_USERNAME_LENGTH = 4;
    private static final int MIN_ROLENAME_LENGTH = 4;
    private static final String ROOT_PREFIX = TsFileDBConstant.PATH_ROOT;
    private static final String ENCRYPT_ALGORITHM = "MD5";
    private static final String STRING_ENCODING = "utf-8";

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
            throw new AuthException(String.format("Illegal path %s, path should start with \"%s\"", path, ROOT_PREFIX));
    }

    public static void validatePrivilegeOnPath(String path, int privilegeId) throws AuthException {
        validatePrivilege(privilegeId);
        PrivilegeType type = PrivilegeType.values()[privilegeId];
        if(!path.equals(TsFileDBConstant.PATH_ROOT)) {
            validatePath(path);
            switch (type) {
                case READ_TIMESERIES:
                case CREATE_TIMESERIES:
                case DELETE_TIMESERIES:
                case INSERT_TIMESERIES:
                case UPDATE_TIMESERIES:
                    return;
                default:
                    throw new AuthException(String.format("Illegal privilege %s on path %s", type.toString(), path));
            }
        } else {
            switch (type) {
                case READ_TIMESERIES:
                case CREATE_TIMESERIES:
                case DELETE_TIMESERIES:
                case INSERT_TIMESERIES:
                case UPDATE_TIMESERIES:
                    validatePath(path);
                default:
                    return;
            }
        }
    }

    public static String encryptPassword(String password) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(ENCRYPT_ALGORITHM);
            messageDigest.update(password.getBytes(STRING_ENCODING));
            return new String(messageDigest.digest(), STRING_ENCODING);
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            return password;
        }
    }

    /**
     *
     * @param pathA
     * @param pathB
     * @return True if pathA == pathB, or pathA is an extension of pathB, e.g. pathA = "root.a.b.c" and pathB = "root.a"
     */
    public static boolean pathBelongsTo(String pathA, String pathB) {
        return pathA.equals(pathB) || (pathA.startsWith(pathB) && pathA.charAt(pathB.length()) == TsFileDBConstant.PATH_SEPARATER);
    }
}
