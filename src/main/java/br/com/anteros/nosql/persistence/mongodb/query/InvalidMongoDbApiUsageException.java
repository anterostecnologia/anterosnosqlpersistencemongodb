package br.com.anteros.nosql.persistence.mongodb.query;

import br.com.anteros.nosql.persistence.session.exception.NoSQLDataAccessException;

public class InvalidMongoDbApiUsageException extends NoSQLDataAccessException {

	public InvalidMongoDbApiUsageException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public InvalidMongoDbApiUsageException(String msg) {
		super(msg);
	}


}
