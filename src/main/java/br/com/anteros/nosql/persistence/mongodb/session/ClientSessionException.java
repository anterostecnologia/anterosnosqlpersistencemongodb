package br.com.anteros.nosql.persistence.mongodb.session;

import br.com.anteros.nosql.persistence.session.exception.NoSQLDataAccessException;

public class ClientSessionException extends NoSQLDataAccessException {

	public ClientSessionException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public ClientSessionException(String msg) {
		super(msg);
	}

}
