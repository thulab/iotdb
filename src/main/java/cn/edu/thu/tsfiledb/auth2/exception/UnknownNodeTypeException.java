package cn.edu.thu.tsfiledb.auth2.exception;

import java.io.IOException;

public class UnknownNodeTypeException extends IOException {

	public UnknownNodeTypeException() {
		super();
	}

	public UnknownNodeTypeException(String message, Throwable cause) {
		super(message, cause);
	}

	public UnknownNodeTypeException(String message) {
		super(message);
	}

	public UnknownNodeTypeException(Throwable cause) {
		super(cause);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -3846467843696114423L;

}
