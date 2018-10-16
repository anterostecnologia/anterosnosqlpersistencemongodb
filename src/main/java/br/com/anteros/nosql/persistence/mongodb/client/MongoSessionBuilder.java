package br.com.anteros.nosql.persistence.mongodb.client;

import br.com.anteros.core.utils.Assert;
import br.com.anteros.nosql.persistence.client.NoSQLConnection;
import br.com.anteros.nosql.persistence.client.NoSQLSessionBuilder;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionEntityManager;
import br.com.anteros.nosql.persistence.mongodb.session.SimpleMongoSession;
import br.com.anteros.nosql.persistence.session.NoSQLSession;
import br.com.anteros.nosql.persistence.session.NoSQLSessionFactory;
import br.com.anteros.nosql.persistence.session.ShowCommandsType;
import br.com.anteros.nosql.persistence.session.transaction.NoSQLTransactionFactory;

public class MongoSessionBuilder implements NoSQLSessionBuilder {

	private NoSQLConnection connection;
	private NoSQLDescriptionEntityManager descriptionEntityManager;
	private ShowCommandsType[] showCommands = { ShowCommandsType.NONE };
	private boolean useBeanValidation = false;
	private boolean formatCommands = true;
	private NoSQLSessionFactory sessionFactory;
	private NoSQLTransactionFactory transactionFactory;
	private boolean withoutTransactionControl;

	@Override
	public NoSQLSessionBuilder sessionFactory(NoSQLSessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
		return this;
	}

	@Override
	public NoSQLSessionBuilder connection(NoSQLConnection connection) {
		this.connection = connection;
		return this;
	}

	@Override
	public NoSQLSessionBuilder descriptionEntityManager(NoSQLDescriptionEntityManager descriptionEntityManager) {
		this.descriptionEntityManager = descriptionEntityManager;
		return this;
	}

	@Override
	public NoSQLSessionBuilder showCommands(ShowCommandsType[] showCommands) {
		this.showCommands = showCommands;
		return this;
	}

	@Override
	public NoSQLSessionBuilder formatCommands(boolean formatCommands) {
		this.formatCommands = formatCommands;
		return this;
	}

	@Override
	public NoSQLSessionBuilder useBeanValidation(boolean useBeanValidation) {
		this.useBeanValidation = useBeanValidation;
		return this;
	}

	@Override
	public NoSQLSessionBuilder transactionFactory(NoSQLTransactionFactory transactionFactory) {
		this.transactionFactory = transactionFactory;
		return this;
	}

	@Override
	public NoSQLSession<?> build() {
		Assert.notNull(connection, "Informe a conexão com o banco de dados.");
		Assert.notNull(descriptionEntityManager, "Informe o descritor de entidades com o banco de dados.");
		Assert.notNull(sessionFactory, "Informe a fábrica de sessões com o banco de dados.");
		return new SimpleMongoSession(sessionFactory, connection, descriptionEntityManager,
				descriptionEntityManager.getDialect(), showCommands, formatCommands, descriptionEntityManager.getDialect().getTransactionFactory(),
				useBeanValidation, withoutTransactionControl);
	}

	@Override
	public NoSQLSessionBuilder withoutTransactionControl(boolean value) {
		this.withoutTransactionControl = value;
		return this;
	}

}
