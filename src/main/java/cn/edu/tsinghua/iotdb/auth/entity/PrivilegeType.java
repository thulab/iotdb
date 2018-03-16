package cn.edu.tsinghua.iotdb.auth.entity;

/**
 * This enum class contains all available privileges in IoTDB.
 */
public enum PrivilegeType {
    SET_STORAGE_GROUP, INSERT_TIMESERIES, UPDATE_TIMESERIES, READ_TIMESERIES, DELETE_TIMESERIES,
    CREATE_USER, DELETE_USER, MODIFY_PASSWORD, LIST_USER, GRANT_USER_PRIVILEGE, REVOKE_USER_PRIVILEGE, GRANT_USER_ROLE, REVOKE_USER_ROLE,
    CREATE_ROLE, DELETE_ROLE, LIST_ROLE, GRANT_ROLE_PRIVILEGE, REVOKE_ROLE_PRIVILEGE;

    public static boolean isPathRelevant(int type) {
        return type <= DELETE_TIMESERIES.ordinal();
    }
}
