package cn.edu.thu.tsfiledb.qp.physical.sys;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.auth2.model.Permission;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.logical.sys.AuthorOperator.AuthorType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

/**
 * @author kangrong
 * @author qiaojialin
 */
public class AuthorPlan extends PhysicalPlan {

	private final AuthorType authorType;
	private String userName;
	private String roleName;
	private String password;
	private String newPassword;
	private long permission;
	private Path nodeName;

	public AuthorPlan(AuthorType authorType, String userName, String roleName, String password, String newPassword,
			String[] authorizationList, Path nodeName) {
		super(false, OperatorType.AUTHOR);
		this.authorType = authorType;
		this.userName = userName;
		this.roleName = roleName;
		this.password = password;
		this.newPassword = newPassword;
		this.permission = strToPermissions(authorizationList);
		this.nodeName = nodeName;

	}

	public AuthorType getAuthorType() {
		return authorType;
	}

	public String getUserName() {
		return userName;
	}
	
	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getRoleName() {
		return roleName;
	}

	public String getPassword() {
		return password;
	}

	public String getNewPassword() {
		return newPassword;
	}

	public long getPermissions() {
		return permission;
	}
	
	public void setPermissions(long permission) {
		this.permission = permission;
	}

	public Path getNodeName() {
		return nodeName;
	}

	private long strToPermissions(String[] authorizationList) {
		long result = Permission.NONE;
		if (authorizationList == null)
			return result;
		for (String s : authorizationList) {
			long permission = Permission.nameToLong(s);
			result = Permission.combine(result, permission);
		}
		return result;
	}

	@Override
	public String toString() {
		return "userName: "+ userName +
				"\nroleName: " + roleName +
				"\npassword: " + password +
				"\nnewPassword: " + newPassword +
				"\npermissions: " + Permission.longToName(permission) +
				"\nnodeName: " + nodeName +
				"\nauthorType: " + authorType;
	}

	@Override
	public List<Path> getPaths() {
		List<Path> ret = new ArrayList<>();
		if (nodeName != null)
			ret.add(nodeName);
		return ret;
	}
	
	@Override
	public String convertResult() {
		switch (authorType) {
		case SHOW_PRIVILEGES:
			return "Privileges are " + Permission.longToName(permission);
		case SHOW_ROLES:
			List<String> roleNames = (List<String>) getResult();
			StringBuilder stringBuilder = new StringBuilder("Roles are");
			for(String roleName : roleNames) {
				stringBuilder.append(" ");
				stringBuilder.append(roleName);
			}
			return stringBuilder.toString();
		default:
			break;
		}
		return super.convertResult();
	}
}
