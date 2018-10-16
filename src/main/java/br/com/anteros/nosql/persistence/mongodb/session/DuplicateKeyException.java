package br.com.anteros.nosql.persistence.mongodb.session;

import br.com.anteros.nosql.persistence.session.exception.NoSQLDataAccessException;

public class DuplicateKeyException extends NoSQLDataAccessException {

	public DuplicateKeyException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public DuplicateKeyException(String msg) {
		super(msg);
	}

}
