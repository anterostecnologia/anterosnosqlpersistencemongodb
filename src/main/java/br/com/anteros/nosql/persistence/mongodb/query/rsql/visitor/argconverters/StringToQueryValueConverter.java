package br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.argconverters;

import br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.structs.ConversionInfo;
import br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.structs.Lazy;

public interface StringToQueryValueConverter {

    Lazy<Object> convert(ConversionInfo info);

}
