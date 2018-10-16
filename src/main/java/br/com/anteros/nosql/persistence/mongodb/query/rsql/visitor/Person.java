package br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor;

import java.util.Calendar;

import br.com.anteros.nosql.persistence.metadata.annotations.Entity;
import br.com.anteros.nosql.persistence.metadata.annotations.Id;
import br.com.anteros.nosql.persistence.metadata.annotations.Property;

@Entity("persons")
public class Person {

    // standard stuff
    @Id
    private String id;
    private int age;
    private int height;
    private String firstName;
    private String lastName;


    // field annotations
    @Property("aGoodFieldName")
    private int aBadFieldName;


    // custom type conversions
    private Calendar dateOfBirth;


}
