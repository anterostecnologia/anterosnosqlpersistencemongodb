package br.com.anteros.nosql.persistence.mongodb.transaction;

import org.springframework.util.ClassUtils;

import com.mongodb.ClientSessionOptions;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;

import br.com.anteros.nosql.persistence.client.NoSQLConnection;
import br.com.anteros.nosql.persistence.mongodb.client.MongoConnection;
import br.com.anteros.nosql.persistence.session.NoSQLPersistenceContext;
import br.com.anteros.nosql.persistence.session.transaction.AbstractNoSQLTransaction;
import br.com.anteros.nosql.persistence.session.transaction.NoSQLTransactionOptions;

public class MongoTransaction extends AbstractNoSQLTransaction {
	
	private ClientSession clientSession;

	public MongoTransaction(NoSQLConnection connection, NoSQLPersistenceContext context) {
		super(connection, context);
//		this.clientSession = ((MongoConnection)connection).getSession(ClientSessionOptions.builder().causallyConsistent(true).build());
	}

	@Override
	protected void doBegin(NoSQLTransactionOptions options) {
		if (options != null)
			clientSession.startTransaction((TransactionOptions) options.getOptions());
		else			
			clientSession.startTransaction();		
	}

	@Override
	protected void doCommit() {
		clientSession.commitTransaction();
		
	}

	@Override
	protected void doRollback() {
		clientSession.abortTransaction();		
	}
	
	@Override
	protected boolean doExtendedActiveCheck() {
		return clientSession.getServerSession() != null && !clientSession.getServerSession().isClosed();
	}
	
	
	public String debugString() {

		if (clientSession == null) {
			return "null";
		}

		String debugString = String.format("[%s@%s ", ClassUtils.getShortName(clientSession.getClass()),
				Integer.toHexString(clientSession.hashCode()));

		try {
			if (clientSession.getServerSession() != null) {
				debugString += String.format("id = %s, ", clientSession.getServerSession().getIdentifier());
				debugString += String.format("causallyConsistent = %s, ", clientSession.isCausallyConsistent());
				debugString += String.format("txActive = %s, ", clientSession.hasActiveTransaction());
				debugString += String.format("txNumber = %d, ", clientSession.getServerSession().getTransactionNumber());
				debugString += String.format("closed = %d, ", clientSession.getServerSession().isClosed());
				debugString += String.format("clusterTime = %s", clientSession.getClusterTime());
			} else {
				debugString += "id = n/a";
				debugString += String.format("causallyConsistent = %s, ", clientSession.isCausallyConsistent());
				debugString += String.format("txActive = %s, ", clientSession.hasActiveTransaction());
				debugString += String.format("clusterTime = %s", clientSession.getClusterTime());
			}
		} catch (RuntimeException e) {
			debugString += String.format("error = %s", e.getMessage());
		}

		debugString += "]";

		return debugString;
	}

	@Override
	public void close() {
		if (clientSession.getServerSession() != null && !clientSession.getServerSession().isClosed()) {
			clientSession.close();
		}		
	}

	public ClientSession getClientSession() {
		return clientSession;
	}

	

}
