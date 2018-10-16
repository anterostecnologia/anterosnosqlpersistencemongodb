package br.com.anteros.nosql.persistence.mongodb.session;

import br.com.anteros.nosql.persistence.session.exception.NoSQLDataAccessException;

public class InvalidDataAccessResourceUsageException extends NoSQLDataAccessException {

	public InvalidDataAccessResourceUsageException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public InvalidDataAccessResourceUsageException(String msg) {
		super(msg);
	}

}
