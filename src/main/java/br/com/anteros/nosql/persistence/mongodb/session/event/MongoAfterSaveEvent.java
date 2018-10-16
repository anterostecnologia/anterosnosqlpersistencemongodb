package br.com.anteros.nosql.persistence.mongodb.session.event;

import org.bson.Document;

import br.com.anteros.nosql.persistence.session.event.AbstractAfterSaveEvent;

public class MongoAfterSaveEvent extends AbstractAfterSaveEvent<Document> {

	public MongoAfterSaveEvent(Object source, Document document, String collectionName) {
		super(source, document, collectionName);
	}

}
