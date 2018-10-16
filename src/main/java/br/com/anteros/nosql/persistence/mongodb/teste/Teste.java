package br.com.anteros.nosql.persistence.mongodb.teste;

import br.com.anteros.nosql.persistence.metadata.configuration.AnterosNoSQLProperties;
import br.com.anteros.nosql.persistence.mongodb.query.MongoCriteria;
import br.com.anteros.nosql.persistence.mongodb.query.MongoQuery;
import br.com.anteros.nosql.persistence.mongodb.session.MongoSession;
import br.com.anteros.nosql.persistence.session.NoSQLSessionFactory;
import br.com.anteros.nosql.persistence.session.configuration.impl.AnterosNoSQLPersistenceConfiguration;

public class Teste {
	
	public static void main(String[] args) throws Exception {
		
		
		NoSQLSessionFactory sessionFactory = AnterosNoSQLPersistenceConfiguration
				.newConfiguration()
				.addProperty(AnterosNoSQLProperties.DIALECT, "br.com.anteros.nosql.persistence.mongodb.dialect.MongoDialect")
				.addProperty(AnterosNoSQLProperties.CONNECTION_HOST, "localhost")
				.addProperty(AnterosNoSQLProperties.CONNECTION_PORT, "27017")
				.addProperty(AnterosNoSQLProperties.DATABASE_NAME, "myDb")
				.addProperty(AnterosNoSQLProperties.CONNECTION_USER, "edson")
				.addProperty(AnterosNoSQLProperties.CONNECTION_PASSWORD, "teste")
				.addAnnotatedClass(Pessoa.class)
				.withoutTransactionControl(true)
				.buildSessionFactory();
		
		MongoSession session = (MongoSession) sessionFactory.getCurrentSession();

		
		
		System.out.println(session.collectionExists("frutas"));
		
		MongoQuery query = MongoQuery.of();
		
		query.addCriteria(MongoCriteria.where("nome").is("Edson Martins"));
		
		session.find(query, Pessoa.class);
		
		
		
		
		
		
		
		

	}

}
