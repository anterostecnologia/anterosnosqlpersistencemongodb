package br.com.anteros.nosql.persistence.mongodb.session;

import br.com.anteros.nosql.persistence.session.exception.NoSQLDataAccessException;

public class UncategorizedMongoDbException extends NoSQLDataAccessException {

	public UncategorizedMongoDbException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public UncategorizedMongoDbException(String msg) {
		super(msg);
	}

	
}
