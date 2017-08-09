package cn.edu.thu.tsfiledb.auth2.exception;

public class NoSuchUserException extends AuthException{

	public NoSuchUserException() {
		super();
	}

	public NoSuchUserException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public NoSuchUserException(String message, Throwable cause) {
		super(message, cause);
	}

	public NoSuchUserException(String message) {
		super(message);
	}

	public NoSuchUserException(Throwable cause) {
		super(cause);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 3722948151631931071L;

}
