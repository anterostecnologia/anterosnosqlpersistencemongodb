package br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import br.com.anteros.nosql.persistence.mongodb.query.MongoCriteria;
import br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.argconverters.NoOpConverter;
import br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.argconverters.OperatorSpecificConverter;
import br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.argconverters.StringToQueryValueConverter;
import br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.structs.ConversionInfo;
import br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.utils.LazyUtils;
import br.com.anteros.nosql.persistence.session.query.NoSQLCriteria;
import br.com.anteros.nosql.persistence.session.query.rsql.ast.ComparisonNode;

public class ComparisonToCriteriaConverter {

    private List<StringToQueryValueConverter> converters = new ArrayList<>();

    public ComparisonToCriteriaConverter() {
        converters.add(new OperatorSpecificConverter());
        converters.add(new NoOpConverter());
    }

    public ComparisonToCriteriaConverter(List<StringToQueryValueConverter> converters) {
        this.converters = converters;
    }

    public NoSQLCriteria<?> asCriteria(ComparisonNode node, Class<?> targetEntityClass) {
        Operator operator = Operator.toOperator(node.getOperator());
        List<Object> arguments = mapArgumentsToAppropriateTypes(operator, node, targetEntityClass);
        return makeCriteria(node.getSelector(), operator, arguments);
    }


    private NoSQLCriteria<?> makeCriteria(String selector, Operator operator, List<Object> arguments) {
        switch (operator) {
            case EQUAL:
                return MongoCriteria.where(selector).is(getFirst(operator, arguments));
            case NOT_EQUAL:
                return MongoCriteria.where(selector).ne(getFirst(operator, arguments));
            case GREATER_THAN:
                return MongoCriteria.where(selector).gt(getFirst(operator, arguments));
            case GREATER_THAN_OR_EQUAL:
                return MongoCriteria.where(selector).gte(getFirst(operator, arguments));
            case LESS_THAN:
                return MongoCriteria.where(selector).lt(getFirst(operator, arguments));
            case LESS_THAN_OR_EQUAL:
                return MongoCriteria.where(selector).lte(getFirst(operator, arguments));
            case REGEX:
                return MongoCriteria.where(selector).regex((String)getFirst(operator, arguments));
            case EXISTS:
                return MongoCriteria.where(selector).exists((Boolean)getFirst(operator, arguments));
            case IN:
                return MongoCriteria.where(selector).in(arguments);
            case NOT_IN:
                return MongoCriteria.where(selector).nin(arguments);
            default:
                // can't happen.
                return null;
        }
    }


    private Object getFirst(Operator operator, List<Object> arguments) {
        if(arguments != null && arguments.size() == 1) {
            return arguments.iterator().next();
        } else {
            throw new UnsupportedOperationException("You cannot perform the query operation " + operator.name() + " with anything except a single value.");
        }
    }

    private List<Object> mapArgumentsToAppropriateTypes(Operator operator, ComparisonNode node, Class<?> targetEntityClass) {
        return node.getArguments().stream().map(arg -> convert(new ConversionInfo()
              .setArgument(arg)
              .setOperator(operator)
              .setPathToField(node.getSelector())
              .setTargetEntityClass(targetEntityClass))).collect(Collectors.toList());
    }


    private Object convert(ConversionInfo conversionInfo) {
        return LazyUtils.firstThatReturnsNonNull(converters.stream()
                .map(converter -> converter.convert(conversionInfo))
                .collect(Collectors.toList()));
    }

}

