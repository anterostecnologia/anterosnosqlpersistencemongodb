package br.com.anteros.nosql.persistence.mongodb.transaction;

import br.com.anteros.nosql.persistence.client.NoSQLConnection;
import br.com.anteros.nosql.persistence.session.NoSQLPersistenceContext;
import br.com.anteros.nosql.persistence.session.transaction.NoSQLTransaction;
import br.com.anteros.nosql.persistence.session.transaction.NoSQLTransactionFactory;

public class MongoTransactionFactory implements NoSQLTransactionFactory {

	@Override
	public NoSQLTransaction createTransaction(NoSQLConnection connection, NoSQLPersistenceContext context) {
		return new MongoTransaction(connection, context);
	}


}
