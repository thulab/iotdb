package cn.edu.tsinghua.iotdb.auth;

import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.iotdb.auth.dao.Authorizer;
import cn.edu.tsinghua.iotdb.auth.model.Permission;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

public class AuthorityChecker {

    private static final String SUPER_USER = "root";
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
        for (int i = 0; i < paths.size(); i++) {
            if (!checkOnePath(username, paths.get(i), permission)) {
                return false;
            }
        }
        return true;
    }

    private static List<String> getAllParentPath(Path path) {
        List<String> parentPaths = new ArrayList<String>();
        String fullPath = path.getFullPath();
        String[] nodes = fullPath.split("\\.");

        for (int i = 0; i < nodes.length; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j <= i; j++) {
                sb.append(nodes[j]);
                if (j < i) {
                    sb.append(".");
                }
            }
            parentPaths.add(sb.toString());
        }
        return parentPaths;
    }

    private static boolean checkOnePath(String username, Path path, int permission) {
        List<String> parentPaths = getAllParentPath(path);
        IAuthorizer authorizer = Authorizer.instance;
        for (int i = 0; i < parentPaths.size(); i++) {
            try {
                if (authorizer.checkUserPermission(username, parentPaths.get(i), permission)) {
                    return true;
                }
            } catch (AuthException e) {
                logger.error("Error occur when checking the path {} for user {}", path, username, e);
            }
        }
        return false;
    }

    private static int translateToPermissionId(Operator.OperatorType type) {
        switch (type) {
            case METADATA:
                return Permission.CREATE;
            case QUERY:
            case SELECT:
            case FILTER:
            case GROUPBY:
            case SEQTABLESCAN:
            case TABLESCAN:
                return Permission.READ;
            case DELETE:
                return Permission.DELETE;
            case INSERT:
            case LOADDATA:
                return Permission.INSERT;
            case UPDATE:
                return Permission.MODIFY;
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

                return -1;
            default:
                return -1;
        }

    }
}
