package br.com.anteros.nosql.persistence.mongodb.session.event;

import org.bson.Document;

import br.com.anteros.nosql.persistence.session.event.AbstractBeforeConvertEvent;

public class MongoBeforeConvertEvent extends AbstractBeforeConvertEvent<Document> {

	public MongoBeforeConvertEvent(Object source, Document document, String collectionName) {
		super(source, document, collectionName);
	}

}
