package br.com.anteros.nosql.persistence.mongodb.session;

import br.com.anteros.nosql.persistence.session.exception.NoSQLDataAccessException;

public class DataIntegrityViolationException extends NoSQLDataAccessException {

	public DataIntegrityViolationException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public DataIntegrityViolationException(String msg) {
		super(msg);
	}

}
