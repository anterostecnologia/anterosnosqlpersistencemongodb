package br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor;

import java.util.List;
import java.util.stream.Collectors;

import br.com.anteros.nosql.persistence.mongodb.query.MongoCriteria;
import br.com.anteros.nosql.persistence.session.query.NoSQLCriteria;
import br.com.anteros.nosql.persistence.session.query.rsql.ast.AndNode;
import br.com.anteros.nosql.persistence.session.query.rsql.ast.ComparisonNode;
import br.com.anteros.nosql.persistence.session.query.rsql.ast.LogicalNode;
import br.com.anteros.nosql.persistence.session.query.rsql.ast.NoArgRSQLVisitorAdapter;
import br.com.anteros.nosql.persistence.session.query.rsql.ast.Node;
import br.com.anteros.nosql.persistence.session.query.rsql.ast.OrNode;

public class CriteriaBuildingVisitor extends NoArgRSQLVisitorAdapter<NoSQLCriteria<?>> {

    private ComparisonToCriteriaConverter converter;
    private Class<?> targetEntityType;

    public CriteriaBuildingVisitor(ComparisonToCriteriaConverter converter, Class<?> targetEntity) {
        this.converter = converter;
        this.targetEntityType = targetEntity;
    }
	
    @Override
    public NoSQLCriteria<?> visit(AndNode node) {
        MongoCriteria parent = MongoCriteria.of();
        List<NoSQLCriteria<?>> children = getChildCriteria(node);
        return parent.andOperator(children.toArray(new NoSQLCriteria[children.size()]));
    }

    @Override
    public NoSQLCriteria<?> visit(OrNode node) {
        MongoCriteria parent = MongoCriteria.of();
        List<NoSQLCriteria<?>> children = getChildCriteria(node);
        return parent.orOperator(children.toArray(new NoSQLCriteria<?>[children.size()]));
    }

    @Override
    public NoSQLCriteria<?> visit(ComparisonNode node) {
        return converter.asCriteria(node, targetEntityType);
    }

    private List<NoSQLCriteria<?>> getChildCriteria(LogicalNode node) {
        return node.getChildren().stream().map(this::visit).collect(Collectors.toList());
    }

    private NoSQLCriteria<?> visit(Node node) {
        if (node instanceof AndNode) {
            return visit((AndNode) node);
        } else if (node instanceof OrNode) {
            return visit((OrNode) node);
        } else{
            return visit((ComparisonNode) node);
        }
    }

}
