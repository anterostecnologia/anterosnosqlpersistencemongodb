package br.com.anteros.nosql.persistence.mongodb.query;

import java.util.ArrayList;
import java.util.List;

import br.com.anteros.nosql.persistence.session.query.Sort;



public class Teste {

	
	public static void main(String[] args) {
		MongoQuery query = MongoQuery.of();
		query.addCriteria(MongoCriteria.where("age").lt(50).gt(20));
		
		System.out.println("1) "+(((MongoQuery)query).getQueryObject()).toJson());
		
		
		MongoQuery query2 = MongoQuery.of();
		query2.addCriteria(MongoCriteria.where("name").is("dog").and("age").is(40));
		
		System.out.println("2) "+query2.getQueryObject().toJson());
		
		
		List<Integer> listOfAge = new ArrayList<Integer>();
		listOfAge.add(10);
		listOfAge.add(30);
		listOfAge.add(40);

		MongoQuery query3 = MongoQuery.of();
		query3.addCriteria(MongoCriteria.where("age").in(listOfAge));
		
		System.out.println("3) "+query3.getQueryObject().toJson());
		
		
		
		MongoQuery query4 = MongoQuery.of();
		query4.addCriteria(
			MongoCriteria.where("age").exists(true)
			.andOperator(
				MongoCriteria.where("age").gt(10),
		                MongoCriteria.where("age").lt(40)
			)
		);
		
		System.out.println("4) "+query4.getQueryObject().toJson());
		
		
		MongoQuery query5 = MongoQuery.of();
		query5.addCriteria(MongoCriteria.where("age").gte(30));
		query5.with(new Sort(Sort.Direction.DESC, "age"));
		
		System.out.println("5) "+query5.getQueryObject().toJson());
		System.out.println("5) "+query5.getSortObject().toJson());
		
		
		MongoQuery query6 = MongoQuery.of();
		query6.addCriteria(MongoCriteria.where("name").regex("D.*G", "i"));

		System.out.println("6) "+query6.getQueryObject().toJson());
		
		
		MongoQuery query7 = MongoQuery.of();
		query7.addCriteria(MongoCriteria.where("user.name").is("_edson"));
		query7.limit(50);
		query7.offSet(10);
		
		System.out.println("7) "+query7.getQueryObject().toJson());

	}
}
