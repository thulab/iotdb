package cn.edu.thu.tsfiledb.auth2.exception;

public class NoSuchPermException extends AuthException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 579363113053904971L;

	public NoSuchPermException() {
		super();
	}

	public NoSuchPermException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public NoSuchPermException(String message, Throwable cause) {
		super(message, cause);
	}

	public NoSuchPermException(String message) {
		super(message);
	}

	public NoSuchPermException(Throwable cause) {
		super(cause);
	}

}
