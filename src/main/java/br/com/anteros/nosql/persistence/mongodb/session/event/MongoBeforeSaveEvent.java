package br.com.anteros.nosql.persistence.mongodb.session.event;

import org.bson.Document;

import br.com.anteros.nosql.persistence.session.event.AbstractBeforeConvertEvent;

public class MongoBeforeSaveEvent extends AbstractBeforeConvertEvent<Document> {

	public MongoBeforeSaveEvent(Object source, Document document, String collectionName) {
		super(source, document, collectionName);
	}

}
