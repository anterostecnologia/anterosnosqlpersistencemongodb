package br.com.anteros.nosql.persistence.mongodb.query;


import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionEntity;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;
import br.com.anteros.nosql.persistence.session.query.NoSQLValidationException;


public class PathTarget {
    private final String path;
    private final List<String> segments;
    private boolean validateNames = true;
    private int position;
    private AbstractNoSQLObjectMapper mapper;
    private NoSQLDescriptionEntity context;
    private NoSQLDescriptionEntity root;
    private NoSQLDescriptionField target;
    private boolean resolved = false;


    public PathTarget(final AbstractNoSQLObjectMapper mapper, final NoSQLDescriptionEntity root, final String path) {
        this.root = root;
        segments = asList(path.split("\\."));
        this.mapper = mapper;
        this.path = path;
    }

  
    public void disableValidation() {
        resolved = false;
        validateNames = false;
    }

    private boolean hasNext() {
        return position < segments.size();
    }

    public String translatedPath() {
        if (!resolved) {
            resolve();
        }
        return join(segments, '.');
    }


    public NoSQLDescriptionField getTarget() {
        if (!resolved) {
            resolve();
        }
        return target;
    }

    String next() {
        return segments.get(position++);
    }

    private void resolve() {
        context = this.root;
        position = 0;
        NoSQLDescriptionField field = null;
        while (hasNext()) {
            String segment = next();

            if (segment.equals("$") || segment.matches("[0-9]+")) {  // array operator
                if (!hasNext()) {
                    return;
                }
                segment = next();
            }
            field = resolveField(segment);

            if (field != null) {
                translate(field.getName());
                if (field.isAnyMap() && hasNext()) {
                    next();  // consume the map key segment
                }
            } else {
                if (validateNames) {
                    throw new NoSQLValidationException(format("Could not resolve path '%s' against '%s'.", join(segments, '.'),
                                                         root.getEntityClass().getName()));
                }
            }
        }
        target = field;
        resolved = true;
    }

    private void translate(final String nameToStore) {
        segments.set(position - 1, nameToStore);
    }

    private NoSQLDescriptionField resolveField(final String segment) {
    	NoSQLDescriptionField mf = context.getDescriptionField(segment);
        if (mf == null) {
            mf = context.getDescriptionFieldByFieldName(segment);
        }
        if (mf == null) {
            Iterator<NoSQLDescriptionEntity> subTypes = Arrays.asList(mapper.getDescriptionEntityManager().getEntitiesBySuperClass(context)).iterator();
            while (mf == null && subTypes.hasNext()) {
                context = subTypes.next();
                mf = resolveField(segment);
            }
        }

        if (mf != null) {
            context = mapper.getDescriptionEntityManager().getDescriptionEntity(mf.getSubClass() != null ? mf.getSubClass() : mf.getConcreteType());
        }
        return mf;
    }

    @Override
    public String toString() {
        return String.format("PathTarget{root=%s, segments=%s, target=%s}", root.getEntityClass().getSimpleName(), segments, target);
    }
    
    public static String join(final List<String> strings, final char delimiter) {
        StringBuilder builder = new StringBuilder();
        for (String element : strings) {
            if (builder.length() != 0) {
                builder.append(delimiter);
            }
            builder.append(element);
        }
        return builder.toString();
    }
}
