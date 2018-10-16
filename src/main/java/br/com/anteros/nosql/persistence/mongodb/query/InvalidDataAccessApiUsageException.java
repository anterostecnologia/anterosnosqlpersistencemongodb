package br.com.anteros.nosql.persistence.mongodb.query;

import br.com.anteros.nosql.persistence.session.exception.NoSQLDataAccessException;

public class InvalidDataAccessApiUsageException extends NoSQLDataAccessException {

	public InvalidDataAccessApiUsageException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public InvalidDataAccessApiUsageException(String msg) {
		super(msg);
	}

}
