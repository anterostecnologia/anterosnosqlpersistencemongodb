package br.com.anteros.nosql.persistence.mongodb.query;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.DBObject;
import com.mongodb.client.model.Filters;

public class MongoDocumentHelper {

	private static final String ID_FIELD = "_id";
	private static final Document ID_ONLY_PROJECTION = new Document(ID_FIELD, 1);

	private Document document;	
	
	private MongoDocumentHelper(Document document) {
		this.document = document;
	}
	

	public static Document getIdOnlyProjection() {
		return ID_ONLY_PROJECTION;
	}

	public static Document getIdIn(Collection<?> ids) {
		return new Document(ID_FIELD, new Document("$in", ids));
	}

	public static List<Object> toIds(Collection<Document> documents) {
		return documents.stream()//
				.map(it -> it.get(ID_FIELD))//
				.collect(Collectors.toList());
	}
	

	public boolean hasId() {
		return document.containsKey(ID_FIELD);
	}

	public boolean hasNonNullId() {
		return hasId() && document.get(ID_FIELD) != null;
	}

	public Object getId() {
		return document.get(ID_FIELD);
	}

	public <T> T getId(Class<T> type) {
		return document.get(ID_FIELD, type);
	}

	public boolean isIdPresent(Class<?> type) {
		return type.isInstance(getId());
	}

	public Bson getIdFilter() {
		return Filters.eq(ID_FIELD, document.get(ID_FIELD));
	}

	public static MongoDocumentHelper of(Document document) {
		return new MongoDocumentHelper(document);
		
	}

}
