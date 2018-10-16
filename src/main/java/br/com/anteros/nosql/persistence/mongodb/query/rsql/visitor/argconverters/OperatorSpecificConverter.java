package br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.argconverters;

import br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.structs.ConversionInfo;
import br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.structs.Lazy;

public class OperatorSpecificConverter implements StringToQueryValueConverter {

    @Override
    public Lazy<Object> convert(ConversionInfo info) {
        switch (info.getOperator()) {
            case REGEX:
                return Lazy.fromValue(info.getArgument());
            case EXISTS:
                return Lazy.fromValue(Boolean.valueOf(info.getArgument()));
            default:
                return Lazy.empty();
        }
    }

}
