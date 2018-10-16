package br.com.anteros.nosql.persistence.mongodb.session;

import org.bson.Document;

import com.mongodb.client.FindIterable;

interface CursorPreparer {

	FindIterable<Document> prepare(FindIterable<Document> cursor);
}
