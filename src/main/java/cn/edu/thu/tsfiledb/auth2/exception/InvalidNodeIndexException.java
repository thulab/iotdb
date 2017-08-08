package cn.edu.thu.tsfiledb.auth2.exception;

import java.io.IOException;

public class InvalidNodeIndexException extends IOException {

	public InvalidNodeIndexException() {
		super();
	}

	public InvalidNodeIndexException(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidNodeIndexException(String message) {
		super(message);
	}

	public InvalidNodeIndexException(Throwable cause) {
		super(cause);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 918422758560994274L;

}
