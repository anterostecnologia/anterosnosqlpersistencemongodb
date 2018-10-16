package br.com.anteros.nosql.persistence.mongodb.session;

import br.com.anteros.nosql.persistence.session.exception.NoSQLDataAccessException;

public class PermissionDeniedDataAccessException extends NoSQLDataAccessException {

	public PermissionDeniedDataAccessException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public PermissionDeniedDataAccessException(String msg) {
		super(msg);
	}

}
