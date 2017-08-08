package cn.edu.thu.tsfiledb.auth2.exception;

public class PathAlreadyExistException extends AuthException {

	public PathAlreadyExistException() {
		super();
	}

	public PathAlreadyExistException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public PathAlreadyExistException(String message, Throwable cause) {
		super(message, cause);
	}

	public PathAlreadyExistException(String message) {
		super(message);
	}

	public PathAlreadyExistException(Throwable cause) {
		super(cause);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 3307373645152575231L;

}
