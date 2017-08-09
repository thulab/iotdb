package cn.edu.thu.tsfiledb.auth2.exception;

public class NoSuchRoleException extends AuthException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2707842812667348642L;

	public NoSuchRoleException() {
	}

	public NoSuchRoleException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public NoSuchRoleException(String message, Throwable cause) {
		super(message, cause);
	}

	public NoSuchRoleException(String message) {
		super(message);
	}

	public NoSuchRoleException(Throwable cause) {
		super(cause);
	}

}
