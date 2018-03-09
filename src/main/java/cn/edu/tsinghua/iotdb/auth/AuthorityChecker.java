package cn.edu.tsinghua.iotdb.auth;

import cn.edu.tsinghua.iotdb.auth.authorizer.IAuthorizer;
import cn.edu.tsinghua.iotdb.auth.authorizer.LocalFileAuthorizer;
import cn.edu.tsinghua.iotdb.auth.entity.PrivilegeType;
import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AuthorityChecker {

    private static final String SUPER_USER = TsFileDBConstant.ADMIN_NAME;
    private static final Logger logger = LoggerFactory.getLogger(AuthorityChecker.class);

    public static boolean check(String username, List<Path> paths, Operator.OperatorType type) {
        if (SUPER_USER.equals(username)) {
            return true;
        }
        int permission = translateToPermissionId(type);
        if (permission == -1) {
            logger.error("OperateType not found. {}", type);
            return false;
        }
        for (Path path : paths) {
            if (!checkOnePath(username, path, permission)) {
                return false;
            }
        }
        return true;
    }

    private static boolean checkOnePath(String username, Path path, int permission) {
        IAuthorizer authorizer = LocalFileAuthorizer.getInstance();
        try {
            if (authorizer.checkUserPrivileges(username, path.getFullPath(), permission)) {
                return true;
            }
        } catch (AuthException e) {
            logger.error("Error occur when checking the path {} for user {}", path, username, e);
        }
        return false;
    }

    private static int translateToPermissionId(Operator.OperatorType type) {
        switch (type) {
            case METADATA:
                return PrivilegeType.CREATE.ordinal();
            case QUERY:
            case SELECT:
            case FILTER:
            case GROUPBY:
            case SEQTABLESCAN:
            case TABLESCAN:
                return PrivilegeType.READ.ordinal();
            case DELETE:
                return PrivilegeType.DELETE.ordinal();
            case INSERT:
            case LOADDATA:
                return PrivilegeType.INSERT.ordinal();
            case UPDATE:
                return PrivilegeType.UPDATE.ordinal();
            case AUTHOR:
            case BASIC_FUNC:
            case FILEREAD:
            case FROM:
            case FUNC:
            case HASHTABLESCAN:
            case JOIN:
            case LIMIT:
            case MERGEJOIN:
            case NULL:
            case ORDERBY:
            case PROPERTY:
            case SFW:
            case UNION:
                logger.error("Illegal operator type authorization : {}", type);
                return -1;
            default:
                return -1;
        }

    }
}
