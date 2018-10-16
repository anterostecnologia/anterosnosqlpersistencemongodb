package br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.argconverters;

import br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.structs.ConversionInfo;
import br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.structs.Lazy;

public class NoOpConverter implements StringToQueryValueConverter {

    @Override
    public Lazy<Object> convert(ConversionInfo info) {
        return Lazy.fromValue(info.getArgument());
    }

}
