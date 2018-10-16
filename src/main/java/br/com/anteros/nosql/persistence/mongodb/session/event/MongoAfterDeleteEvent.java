package br.com.anteros.nosql.persistence.mongodb.session.event;

import org.bson.Document;

import br.com.anteros.nosql.persistence.session.event.AbstractAfterDeleteEvent;

public class MongoAfterDeleteEvent extends AbstractAfterDeleteEvent<Document> {
	
	private static final long serialVersionUID = 1L;

	public MongoAfterDeleteEvent(Document document, Class<?> type, String collectionName) {
		super(document, type, collectionName);
	}

}
