package org.luming.mybatis.plugin.exceptions;

/**
 * @author luming
 */
public class InterceptorException extends RuntimeException {
	private static final long serialVersionUID = 801826594581701061L;

	public InterceptorException() {
		super();
	}

	public InterceptorException(String message) {
		super(message);
	}

	public InterceptorException(String message, Throwable cause) {
		super(message, cause);
	}

	public InterceptorException(Throwable cause) {
		super(cause);
	}
}