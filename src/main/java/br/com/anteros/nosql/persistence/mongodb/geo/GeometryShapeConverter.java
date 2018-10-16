package br.com.anteros.nosql.persistence.mongodb.geo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import br.com.anteros.nosql.persistence.converters.NoSQLSimpleValueConverter;
import br.com.anteros.nosql.persistence.converters.NoSQLTypeConverter;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;

public class GeometryShapeConverter extends NoSQLTypeConverter implements NoSQLSimpleValueConverter {
	private final GeoJsonType geoJsonType;
	private final List<GeometryFactory> factories;

	GeometryShapeConverter(final GeoJsonType... geoJsonTypes) {
		super(geoJsonTypes[0].getTypeClass());
		geoJsonType = geoJsonTypes[0];
		this.factories = Arrays.<GeometryFactory>asList(geoJsonTypes);
	}

	@Override
	public Geometry decode(final Class<?> targetClass, final Object fromDBObject,
			final NoSQLDescriptionField descriptionField) {
		return decodeObject((List) ((DBObject) fromDBObject).get("coordinates"), factories);
	}

	@Override
	public Object encode(final Object value, final NoSQLDescriptionField descriptionField) {
		if (value != null) {
			Object encodedObjects = encodeObjects(((Geometry) value).getCoordinates());
			return new BasicDBObject("type", geoJsonType.getType()).append("coordinates", encodedObjects);
		} else {
			return null;
		}
	}


	@SuppressWarnings("unchecked") // always have unchecked casts when dealing with raw classes
	private Geometry decodeObject(final List mongoDBGeometry, final List<GeometryFactory> geometryFactories) {
		GeometryFactory factory = geometryFactories.get(0);
		if (geometryFactories.size() == 1) {
			// This should be the last list, so no need to decode further
			return factory.createGeometry(mongoDBGeometry);
		} else {
			List<Geometry> decodedObjects = new ArrayList<Geometry>();
			for (final Object objectThatNeedsDecoding : mongoDBGeometry) {
				// MongoDB geometries are lists of lists of lists...
				decodedObjects.add(decodeObject((List) objectThatNeedsDecoding,
						geometryFactories.subList(1, geometryFactories.size())));
			}
			return factory.createGeometry(decodedObjects);
		}
	}

	private Object encodeObjects(final List value) {
		List<Object> encodedObjects = new ArrayList<Object>();
		for (final Object object : value) {
			if (object instanceof Geometry) {
				// iterate through the list of geometry objects recursively until you find the
				// lowest-level
				encodedObjects.add(encodeObjects(((Geometry) object).getCoordinates()));
			} else {
				encodedObjects.add(getMapper().getConverters().encode(object));
			}
		}
		return encodedObjects;
	}

	public static class MultiPolygonConverter extends GeometryShapeConverter {
		public MultiPolygonConverter() {
			super(GeoJsonType.MULTI_POLYGON, GeoJsonType.POLYGON, GeoJsonType.LINE_STRING, GeoJsonType.POINT);
		}
	}

	public static class PolygonConverter extends GeometryShapeConverter {
		public PolygonConverter() {
			super(GeoJsonType.POLYGON, GeoJsonType.LINE_STRING, GeoJsonType.POINT);
		}
	}

	public static class MultiLineStringConverter extends GeometryShapeConverter {
		public MultiLineStringConverter() {
			super(GeoJsonType.MULTI_LINE_STRING, GeoJsonType.LINE_STRING, GeoJsonType.POINT);
		}
	}

	public static class MultiPointConverter extends GeometryShapeConverter {
		public MultiPointConverter() {
			super(GeoJsonType.MULTI_POINT, GeoJsonType.POINT);
		}
	}

	public static class LineStringConverter extends GeometryShapeConverter {

		public LineStringConverter() {
			super(GeoJsonType.LINE_STRING, GeoJsonType.POINT);
		}
	}

	public static class PointConverter extends GeometryShapeConverter {

		public PointConverter() {
			super(GeoJsonType.POINT);
		}
	}
}
