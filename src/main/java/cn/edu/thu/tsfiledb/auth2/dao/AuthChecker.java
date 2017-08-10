package cn.edu.thu.tsfiledb.auth2.dao;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.auth.AuthorityChecker;
import cn.edu.thu.tsfiledb.auth2.exception.AuthException;
import cn.edu.thu.tsfiledb.auth2.model.Permission;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.logical.sys.AuthorOperator.AuthorType;

public class AuthChecker {
	private static final String SUPER_USER = "root";
    private static final Logger logger = LoggerFactory.getLogger(AuthorityChecker.class);
    
    private static boolean isSUPER(String username) {
    	return username.equals(SUPER_USER);
    }
	
    public static boolean check(String username, List<Path> paths, AuthorType authorType) throws AuthException {
    	if (isSUPER(username)) {
            return true;
        }
        long permission = translateToPermissionId(authorType);
        if (permission == -1) {
            logger.error("OperateType not found. {}", authorType);
            return false;
        }
        for (int i = 0; i < paths.size(); i++) {
            if (!checkOnePath(username, paths.get(i), permission)) {
                return false;
            }
        }
        return true;
	}

	public static boolean check(String username, List<Path> paths, OperatorType type) throws AuthException {
		 if (isSUPER(username)) {
	            return true;
	        }
	        long permission = translateToPermissionId(type);
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
	
	private static boolean checkOnePath(String username, Path path, long permission) throws AuthException {
		return AuthDao.getInstance().checkPermissionOnPath(username, path.getFullPath(), permission);
	
    }

	 private static long translateToPermissionId(OperatorType type) {
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
	    
		private static long translateToPermissionId(AuthorType authorType) {
			switch (authorType) {
			case SHOW_ROLES:
			case SHOW_PRIVILEGES:
				return Permission.NONE;
			default:
				return Permission.ADMIN;
			}
		}
}
