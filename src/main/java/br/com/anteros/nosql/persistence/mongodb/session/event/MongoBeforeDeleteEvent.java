package br.com.anteros.nosql.persistence.mongodb.session.event;

import org.bson.Document;

import br.com.anteros.nosql.persistence.session.event.AbstractBeforeDeleteEvent;

public class MongoBeforeDeleteEvent extends AbstractBeforeDeleteEvent<Document> {

	public MongoBeforeDeleteEvent(Document document, Class<?> type, String collectionName) {
		super(document, type, collectionName);
	}

}
