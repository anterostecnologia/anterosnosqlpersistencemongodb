package br.com.anteros.nosql.persistence.mongodb.session.event;

import org.bson.Document;

import br.com.anteros.nosql.persistence.session.event.AbstractBeforeLoadEvent;

public class MongoBeforeLoadEvent extends AbstractBeforeLoadEvent<Document> {

	private static final long serialVersionUID = 1L;

	public MongoBeforeLoadEvent(Object source, Document document, String collectionName) {
		super(source, document, collectionName);
	}

}
