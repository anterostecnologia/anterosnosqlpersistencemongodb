package br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.bson.json.JsonWriterSettings;

import br.com.anteros.nosql.persistence.mongodb.query.MongoCriteria;
import br.com.anteros.nosql.persistence.session.query.NoSQLCriteria;
import br.com.anteros.nosql.persistence.session.query.rsql.RSQLParser;
import br.com.anteros.nosql.persistence.session.query.rsql.ast.Node;

public class RsqlMongoAdapter {

	private ComparisonToCriteriaConverter converter;
	private RSQLParser parser;

	public RsqlMongoAdapter(ComparisonToCriteriaConverter converter) {
		this.converter = converter;
		this.parser = new RSQLParser(
				Arrays.stream(Operator.values()).map(op -> op.operator).collect(Collectors.toSet()));
	}

	public NoSQLCriteria<?> getCriteria(String rsql, Class<?> targetEntityType) {
		Node node = parser.parse(rsql);
		CriteriaBuildingVisitor visitor = new CriteriaBuildingVisitor(converter, targetEntityType);
		return node.accept(visitor);
	}

	public static void main(String[] args) {
		RsqlMongoAdapter adapter = new RsqlMongoAdapter(new ComparisonToCriteriaConverter());
		MongoCriteria criteria = (MongoCriteria) adapter.getCriteria(
				"((firstName==john;lastName==doe),(firstName==aaron;lastName==carter));((age==21;height==90),(age==30;height==100))",
				Person.class);

		System.out.println(criteria.getCriteriaObject().toJson(JsonWriterSettings.builder().indent(true).build()));

	}

}
