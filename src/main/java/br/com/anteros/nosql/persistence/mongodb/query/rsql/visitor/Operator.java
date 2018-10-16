package br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor;

import java.util.Arrays;

import br.com.anteros.nosql.persistence.session.query.rsql.ast.ComparisonOperator;
import br.com.anteros.nosql.persistence.session.query.rsql.ast.RSQLOperators;

public enum Operator {
    EQUAL(RSQLOperators.EQUAL),
    NOT_EQUAL(RSQLOperators.NOT_EQUAL),
    GREATER_THAN(RSQLOperators.GREATER_THAN),
    GREATER_THAN_OR_EQUAL(RSQLOperators.GREATER_THAN_OR_EQUAL),
    LESS_THAN(RSQLOperators.LESS_THAN),
    LESS_THAN_OR_EQUAL(RSQLOperators.LESS_THAN_OR_EQUAL),
    IN(RSQLOperators.IN),
    NOT_IN(RSQLOperators.NOT_IN),
    REGEX(new ComparisonOperator("=re=", false)),
    EXISTS(new ComparisonOperator("=ex=", false));

    public ComparisonOperator operator;
    Operator(ComparisonOperator operator) {
        this.operator = operator;
    }

    public static Operator toOperator(ComparisonOperator operator) {
        return Arrays.stream(values()).filter(value -> value.operator.equals(operator)).findFirst().orElse(null);
    }


}
