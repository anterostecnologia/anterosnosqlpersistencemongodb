package br.com.anteros.nosql.persistence.mongodb.aggregation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;

import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;

import br.com.anteros.core.log.Logger;
import br.com.anteros.core.log.LoggerProvider;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionEntity;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;
import br.com.anteros.nosql.persistence.mongodb.geo.GeometryShapeConverter;
import br.com.anteros.nosql.persistence.mongodb.query.MongoIterator;
import br.com.anteros.nosql.persistence.session.NoSQLSession;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;
import br.com.anteros.nosql.persistence.session.query.NoSQLQuery;
import br.com.anteros.nosql.persistence.session.query.Sort;
import br.com.anteros.nosql.persistence.session.query.Sort.Direction;
import br.com.anteros.nosql.persistence.session.query.Sort.Order;


public class AggregationPipelineImpl implements AggregationPipeline {
	private static Logger LOG = LoggerProvider.getInstance().getLogger(AggregationPipelineImpl.class);

    private final MongoCollection<Document> collection;
    private final Class<?> source;
    private final List<Document> stages = new ArrayList<Document>();
    private final AbstractNoSQLObjectMapper mapper;
    private final NoSQLSession<Document> session;
    private boolean firstStage = false;


    public AggregationPipelineImpl(final NoSQLSession<Document> session, AbstractNoSQLObjectMapper mapper, final MongoCollection<Document> collection, final Class source) {
        this.session = session;
        this.collection = collection;
        this.mapper = mapper;
        this.source = source;
    }


    public List<Document> getStages() {
        return stages;
    }

    @Override
    public <U> Iterator<U> aggregate(final Class<U> target) {
        return aggregate(target, AggregationOptions.builder().build(), collection.getReadPreference());
    }

    @Override
    public <U> Iterator<U> aggregate(final Class<U> target, final AggregationOptions options) {
        return aggregate(target, options, collection.getReadPreference());
    }

    @Override
    public <U> Iterator<U> aggregate(final Class<U> target, final AggregationOptions options, final ReadPreference readPreference) {
    	NoSQLDescriptionEntity descriptionEntity = session.getDescriptionEntityManager().getDescriptionEntity(target);
        return aggregate(descriptionEntity.getCollectionName(), target, options, readPreference);
    }

    @Override
    public <U> Iterator<U> aggregate(final String collectionName, final Class<U> target, final AggregationOptions options,
                                     final ReadPreference readPreference) {
        LOG.debug("stages = " + stages);

        AggregateIterable<Document> cursor = collection.aggregate(stages);
        return new MongoIterator<U, U>(session, cursor.iterator(), mapper, target, collectionName, mapper.createEntityCache());
    }

    @Override
    @SuppressWarnings("deprecation")
    public AggregationPipeline geoNear(final GeoNear geoNear) {
        DBObject geo = new BasicDBObject();
        GeometryShapeConverter.PointConverter pointConverter = new GeometryShapeConverter.PointConverter();
        pointConverter.setMapper(mapper);

        putIfNull(geo, "near", geoNear.getNearAsDBObject(pointConverter));
        putIfNull(geo, "distanceField", geoNear.getDistanceField());
        putIfNull(geo, "limit", geoNear.getLimit());
        putIfNull(geo, "num", geoNear.getMaxDocuments());
        putIfNull(geo, "maxDistance", geoNear.getMaxDistance());
        if (geoNear.getQuery() != null) {
            geo.put("query", geoNear.getQuery().getQueryObject());
        }
        putIfNull(geo, "spherical", geoNear.getSpherical());
        putIfNull(geo, "distanceMultiplier", geoNear.getDistanceMultiplier());
        putIfNull(geo, "includeLocs", geoNear.getIncludeLocations());
        stages.add(new Document("$geoNear", geo));

        return this;
    }

    @Override
    public AggregationPipeline group(final Group... groupings) {
        return group((String) null, groupings);
    }

    @Override
    public AggregationPipeline group(final String id, final Group... groupings) {
        DBObject group = new BasicDBObject();
        group.put("_id", id != null ? "$" + id : null);
        for (Group grouping : groupings) {
            group.putAll(toDBObject(grouping));
        }

        stages.add(new Document("$group", group));
        return this;
    }

    @Override
    public AggregationPipeline group(final List<Group> id, final Group... groupings) {
        DBObject idGroup = null;
        if (id != null) {
            idGroup = new BasicDBObject();
            for (Group group : id) {
                idGroup.putAll(toDBObject(group));
            }
        }
        DBObject group = new BasicDBObject("_id", idGroup);
        for (Group grouping : groupings) {
            group.putAll(toDBObject(grouping));
        }

        stages.add(new Document("$group", group));
        return this;
    }

    @Override
    public AggregationPipeline limit(final int count) {
        stages.add(new Document("$limit", count));
        return this;
    }

    @Override
    public AggregationPipeline lookup(final String from, final String localField, final String foreignField, final String as) {
        stages.add(new Document("$lookup", new Document("from", from)
            .append("localField", localField)
            .append("foreignField", foreignField)
            .append("as", as)));
        return this;
    }

    @Override
    public <Q> AggregationPipeline match(final NoSQLQuery<Q> query) {
        stages.add(new Document("$match", query.getQueryObject()));
        return this;
    }

    @Override
    public <U> Iterator<U> out(final Class<U> target) {
    	NoSQLDescriptionEntity descriptionEntity = session.getDescriptionEntityManager().getDescriptionEntity(target);
        return out(descriptionEntity.getCollectionName(), target);
    }

    @Override
    public <U> Iterator<U> out(final Class<U> target, final AggregationOptions options) {
    	NoSQLDescriptionEntity descriptionEntity = session.getDescriptionEntityManager().getDescriptionEntity(target);
        return out(descriptionEntity.getCollectionName(), target, options);
    }

    @Override
    public <U> Iterator<U> out(final String collectionName, final Class<U> target) {
        return out(collectionName, target, AggregationOptions.builder().build());
    }

    @Override
    public <U> Iterator<U> out(final String collectionName, final Class<U> target, final AggregationOptions options) {
        stages.add(new Document("$out", collectionName));
        return aggregate(target, options);
    }

    @Override
    public AggregationPipeline project(final Projection... projections) {
        firstStage = stages.isEmpty();
        DBObject dbObject = new BasicDBObject();
        for (Projection projection : projections) {
            dbObject.putAll(toDBObject(projection));
        }
        stages.add(new Document("$project", dbObject));
        return this;
    }

    @Override
    public AggregationPipeline skip(final int count) {
        stages.add(new Document("$skip", count));
        return this;
    }

    @Override
    public AggregationPipeline sort(final Sort... sorts) {
        DBObject sortList = new BasicDBObject();
        for (Sort sort : sorts) {
            Iterator<Order> iterator = sort.iterator();
            while (iterator.hasNext()) {
            	Order order = iterator.next();
            	sortList.put(order.getProperty(), order.getDirection()==Direction.ASC?1:-1);
            }
        }

        stages.add(new Document("$sort", sortList));
        return this;
    }

    @Override
    public AggregationPipeline unwind(final String field) {
        stages.add(new Document("$unwind", "$" + field));
        return this;
    }

    @SuppressWarnings("unchecked")
    private DBObject toDBObject(final Projection projection) {
        String target;
        if (firstStage) {
            NoSQLDescriptionField field = mapper.getDescriptionEntityManager().getDescriptionEntity(source).getDescriptionField(projection.getTarget());
            target = field != null ? field.getName() : projection.getTarget();
        } else {
            target = projection.getTarget();
        }

        if (projection.getProjections() != null) {
            List<Projection> list = projection.getProjections();
            DBObject projections = new BasicDBObject();
            for (Projection subProjection : list) {
                projections.putAll(toDBObject(subProjection));
            }
            return new BasicDBObject(target, projections);
        } else if (projection.getSource() != null) {
            return new BasicDBObject(target, projection.getSource());
        } else if (projection.getArguments() != null) {
            if (target == null) {
                return toExpressionArgs(projection.getArguments());
            } else {
                return new BasicDBObject(target, toExpressionArgs(projection.getArguments()));
            }
        } else {
            return new BasicDBObject(target, projection.isSuppressed() ? 0 : 1);
        }
    }

    private DBObject toDBObject(final Group group) {
        BasicDBObject dbObject = new BasicDBObject();

        if (group.getAccumulator() != null) {
            dbObject.put(group.getName(), group.getAccumulator().toDBObject());
        } else if (group.getProjections() != null) {
            final BasicDBObject projection = new BasicDBObject();
            for (Projection p : group.getProjections()) {
                projection.putAll(toDBObject(p));
            }
            dbObject.put(group.getName(), projection);
        } else if (group.getNested() != null) {
            dbObject.put(group.getName(), toDBObject(group.getNested()));
        } else {
            dbObject.put(group.getName(), group.getSourceField());
        }

        return dbObject;
    }

    private void putIfNull(final DBObject dbObject, final String name, final Object value) {
        if (value != null) {
            dbObject.put(name, value);
        }
    }

    private DBObject toExpressionArgs(final List<Object> args) {
        BasicDBList result = new BasicDBList();
        for (Object arg : args) {
            if (arg instanceof Projection) {
                Projection projection = (Projection) arg;
                if (projection.getArguments() != null || projection.getProjections() != null || projection.getSource() != null) {
                    result.add(toDBObject(projection));
                } else {
                    result.add("$" + projection.getTarget());
                }
            } else {
                result.add(arg);
            }
        }
        return result.size() == 1 ? (DBObject) result.get(0) : result;
    }

    @Override
    public String toString() {
        return stages.toString();
    }
}
