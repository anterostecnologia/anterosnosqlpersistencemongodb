package br.com.anteros.nosql.persistence.mongodb.geo;

public enum CoordinateReferenceSystemType {

	NAME("name"),

	LINK("link");

	private final String typeName;

	CoordinateReferenceSystemType(final String typeName) {
		this.typeName = typeName;
	}

	public String getTypeName() {
		return typeName;
	}
}
