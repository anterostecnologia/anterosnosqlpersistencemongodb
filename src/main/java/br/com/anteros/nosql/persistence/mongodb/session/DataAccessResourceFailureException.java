package br.com.anteros.nosql.persistence.mongodb.session;

import br.com.anteros.nosql.persistence.session.exception.NoSQLDataAccessException;

public class DataAccessResourceFailureException extends NoSQLDataAccessException {

	public DataAccessResourceFailureException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public DataAccessResourceFailureException(String msg) {
		super(msg);
	}

}
