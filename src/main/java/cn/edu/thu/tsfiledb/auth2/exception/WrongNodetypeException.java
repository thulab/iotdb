package cn.edu.thu.tsfiledb.auth2.exception;

public class WrongNodetypeException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8098359585172582614L;

	public WrongNodetypeException() {
		super();
	}

	public WrongNodetypeException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public WrongNodetypeException(String message, Throwable cause) {
		super(message, cause);
	}

	public WrongNodetypeException(String message) {
		super(message);
	}

	public WrongNodetypeException(Throwable cause) {
		super(cause);
	}

}
