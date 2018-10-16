package br.com.anteros.nosql.persistence.mongodb.aggregation;

import com.mongodb.DBObject;

interface AggregationElement {
   
    DBObject toDBObject();
}
