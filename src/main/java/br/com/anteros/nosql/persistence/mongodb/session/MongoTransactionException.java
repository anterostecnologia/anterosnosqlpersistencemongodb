package br.com.anteros.nosql.persistence.mongodb.session;

import br.com.anteros.nosql.persistence.session.exception.NoSQLDataAccessException;

public class MongoTransactionException extends NoSQLDataAccessException {

	public MongoTransactionException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public MongoTransactionException(String msg) {
		super(msg);
	}

}
