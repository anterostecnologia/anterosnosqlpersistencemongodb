package br.com.anteros.nosql.persistence.mongodb.session;

import br.com.anteros.nosql.persistence.session.exception.NoSQLDataAccessException;

public class BulkOperationException extends NoSQLDataAccessException {

	public BulkOperationException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public BulkOperationException(String msg) {
		super(msg);
	}

}
