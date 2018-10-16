package br.com.anteros.nosql.persistence.mongodb.session.event;

import org.bson.Document;

import br.com.anteros.nosql.persistence.session.event.NoSQLEvent;
import br.com.anteros.nosql.persistence.session.event.NoSQLEventPublisher;

public class MongoEventPublisher implements NoSQLEventPublisher<Document> {

	public MongoEventPublisher() {

	}

	@Override
	public void publishEvent(NoSQLEvent<Document> event) {

	}

}
