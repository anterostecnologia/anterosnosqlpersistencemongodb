package br.com.anteros.nosql.persistence.mongodb.session.event;

import org.bson.Document;

import br.com.anteros.nosql.persistence.session.event.AbstractAfterLoadEvent;

public class MongoAfterLoadEvent extends AbstractAfterLoadEvent<Document> {

	public MongoAfterLoadEvent(Document document, Class<?> type, String collectionName) {
		super(document, type, collectionName);
	}

}
