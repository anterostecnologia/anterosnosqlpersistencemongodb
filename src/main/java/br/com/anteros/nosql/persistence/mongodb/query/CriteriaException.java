package br.com.anteros.nosql.persistence.mongodb.query;

public class CriteriaException extends RuntimeException {

	public CriteriaException() {
		super();
	}

	public CriteriaException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public CriteriaException(String message, Throwable cause) {
		super(message, cause);
	}

	public CriteriaException(String message) {
		super(message);
	}

	public CriteriaException(Throwable cause) {
		super(cause);
	}

}
