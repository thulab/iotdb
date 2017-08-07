package cn.edu.thu.tsfiledb.auth2.exception;

import java.io.IOException;

public class WriteObjectException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8913608185239367250L;

	public WriteObjectException() {
		super();
	}

	public WriteObjectException(String message, Throwable cause) {
		super(message, cause);
	}

	public WriteObjectException(String message) {
		super(message);
	}

	public WriteObjectException(Throwable cause) {
		super(cause);
	}

}
