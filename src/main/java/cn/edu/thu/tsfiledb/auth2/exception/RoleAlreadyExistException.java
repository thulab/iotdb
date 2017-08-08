package cn.edu.thu.tsfiledb.auth2.exception;

public class RoleAlreadyExistException extends AuthException {

	private static final long serialVersionUID = -5819742872055007657L;

	public RoleAlreadyExistException() {
		super();
	}

	public RoleAlreadyExistException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public RoleAlreadyExistException(String message, Throwable cause) {
		super(message, cause);
	}

	public RoleAlreadyExistException(String message) {
		super(message);
	}

	public RoleAlreadyExistException(Throwable cause) {
		super(cause);
	}

}
