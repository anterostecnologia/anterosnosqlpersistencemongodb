package br.com.anteros.nosql.persistence.mongodb.session.event;

import org.bson.Document;

import br.com.anteros.nosql.persistence.session.event.AbstractAfterConvertEvent;

public class MongoAfterConvertEvent extends AbstractAfterConvertEvent<Document> {
	
	public MongoAfterConvertEvent(Object source, Document document, String collectionName) {
		super(source, document, collectionName);
	}

}
