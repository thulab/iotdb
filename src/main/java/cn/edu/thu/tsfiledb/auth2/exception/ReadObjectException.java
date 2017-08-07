package cn.edu.thu.tsfiledb.auth2.exception;

import java.io.IOException;

public class ReadObjectException extends IOException {

	public ReadObjectException() {
		super();
	}

	public ReadObjectException(String message, Throwable cause) {
		super(message, cause);
	}

	public ReadObjectException(String message) {
		super(message);
	}

	public ReadObjectException(Throwable cause) {
		super(cause);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -4501854349664492093L;

}
