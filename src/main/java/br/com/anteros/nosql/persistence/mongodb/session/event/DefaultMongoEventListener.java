package br.com.anteros.nosql.persistence.mongodb.session.event;

import org.bson.Document;

import br.com.anteros.nosql.persistence.session.event.AbstractNoSQLEventListener;
import br.com.anteros.nosql.persistence.session.event.NoSQLEvent;

public class DefaultMongoEventListener extends AbstractNoSQLEventListener<Document>{

	@Override
	public void onEvent(NoSQLEvent<Document> event) {
		// TODO Auto-generated method stub
		super.onEvent(event);
	}
}
