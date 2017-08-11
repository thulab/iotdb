package cn.edu.thu.tsfiledb.auth2.dao;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.auth2.exception.AuthException;
import cn.edu.thu.tsfiledb.auth2.model.Permission;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.logical.sys.AuthorOperator.AuthorType;

/** This class is specifically used to examine if a user can perform
 * 	certain operations on a list of path. 
 * @author jt
 *
 */
public class AuthChecker {
	private static final String SUPER_USER = "root";
	private static final Logger logger = LoggerFactory.getLogger(AuthChecker.class);

	/** Check if a user "username" is a super user.
	 *  Current method is to simply compare with preset super user name.
	 *  TODO add support to multiple super users
	 * @param username
	 * @return
	 */
	private static boolean isSUPER(String username) {
		return username.equals(SUPER_USER);
	}

	/** Check if a user "username" can perform operation "authorType" on "paths".
	 *  Super user will instantly pass the test.
	 * @param username
	 * @param paths
	 * @param authorType
	 * @return true if the user has such permission,
	 * 			false if the user does not have the permission or the operation type is unsupported
	 * @throws AuthException
	 */
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

	/** Check if a user "username" can perform operation "type" on "paths".
	 *  Super user will instantly pass the test.
	 * @param username
	 * @param paths
	 * @param type
	 * @return true if the user has such permission,
	 * 			false if the user does not have the permission or the operation type is unsupported
	 * @throws AuthException
	 */
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

	/** Check if a user "username" has "permission" on "path".
	 *  For more details, see to AuthDao.checkPermissionOnPath().
	 * @param username
	 * @param path
	 * @param permission
	 * @return
	 * @throws AuthException
	 */
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
