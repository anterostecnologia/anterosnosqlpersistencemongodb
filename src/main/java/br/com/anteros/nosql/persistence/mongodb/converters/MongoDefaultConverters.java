package br.com.anteros.nosql.persistence.mongodb.converters;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import br.com.anteros.nosql.persistence.converters.BigDecimalConverter;
import br.com.anteros.nosql.persistence.converters.BooleanConverter;
import br.com.anteros.nosql.persistence.converters.ByteConverter;
import br.com.anteros.nosql.persistence.converters.CharArrayConverter;
import br.com.anteros.nosql.persistence.converters.CharacterConverter;
import br.com.anteros.nosql.persistence.converters.ClassConverter;
import br.com.anteros.nosql.persistence.converters.DateConverter;
import br.com.anteros.nosql.persistence.converters.DoubleConverter;
import br.com.anteros.nosql.persistence.converters.EnumConverter;
import br.com.anteros.nosql.persistence.converters.EnumSetConverter;
import br.com.anteros.nosql.persistence.converters.FloatConverter;
import br.com.anteros.nosql.persistence.converters.IdentityConverter;
import br.com.anteros.nosql.persistence.converters.InstantConverter;
import br.com.anteros.nosql.persistence.converters.IntegerConverter;
import br.com.anteros.nosql.persistence.converters.LocalDateConverter;
import br.com.anteros.nosql.persistence.converters.LocalDateTimeConverter;
import br.com.anteros.nosql.persistence.converters.LocalTimeConverter;
import br.com.anteros.nosql.persistence.converters.LocaleConverter;
import br.com.anteros.nosql.persistence.converters.LongConverter;
import br.com.anteros.nosql.persistence.converters.MapOfValuesConverter;
import br.com.anteros.nosql.persistence.converters.NoSQLConverters;
import br.com.anteros.nosql.persistence.converters.NoSQLTypeConverter;
import br.com.anteros.nosql.persistence.converters.ShortConverter;
import br.com.anteros.nosql.persistence.converters.StringConverter;
import br.com.anteros.nosql.persistence.converters.TimestampConverter;
import br.com.anteros.nosql.persistence.converters.URIConverter;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;

public class MongoDefaultConverters extends NoSQLConverters {
	public static final boolean JAVA_8;
	private final IdentityConverter identityConverter;
	private final MongoSerializedObjectConverter serializedConverter;

	static {
		boolean found;
		try {
			Class.forName("java.time.LocalDateTime");
			found = true;
		} catch (ClassNotFoundException e) {
			found = false;
		}
		JAVA_8 = found;
	}

	public MongoDefaultConverters(final AbstractNoSQLObjectMapper mapper) {
		super(mapper);
		addConverter(new IdentityConverter(DBObject.class, BasicDBObject.class));
		addConverter(new EnumSetConverter());
		addConverter(new EnumConverter());
		addConverter(new StringConverter());
		addConverter(new CharacterConverter());
		addConverter(new MongoByteConverter());
		addConverter(new BooleanConverter());
		addConverter(new DoubleConverter());
		addConverter(new FloatConverter());
		addConverter(new LongConverter());
		addConverter(new LocaleConverter());
		addConverter(new ShortConverter());
		addConverter(new IntegerConverter());
		addConverter(new CharArrayConverter());
		addConverter(new DateConverter());
		addConverter(new URIConverter());
		addConverter(new MongoKeyConverter());
		addConverter(new MapOfValuesConverter());
		addConverter(new MongoIterableConverter());
		addConverter(new ClassConverter());
		addConverter(new MongoObjectIdConverter());
		addConverter(new TimestampConverter());
		addConverter(new BigDecimalConverter());

		// Converters for Geo entities
//        addConverter(new GeometryShapeConverter.PointConverter());
//        addConverter(new GeometryShapeConverter.LineStringConverter());
//        addConverter(new GeometryShapeConverter.MultiPointConverter());
//        addConverter(new GeometryShapeConverter.MultiLineStringConverter());
//        addConverter(new GeometryShapeConverter.PolygonConverter());
//        addConverter(new GeometryShapeConverter.MultiPolygonConverter());
//        addConverter(new GeometryConverter());

		// generic converter that will just pass things through.
		identityConverter = new IdentityConverter();
		serializedConverter = new MongoSerializedObjectConverter();

		if (JAVA_8) {
			addConverter(LocalTimeConverter.class);
			addConverter(LocalDateTimeConverter.class);
			addConverter(LocalDateConverter.class);
			addConverter(InstantConverter.class);
			addConverter(new OptionalConverter(this));
		}
	}

	@Override
	protected NoSQLTypeConverter getEncoder(final Class<?> c) {
		NoSQLTypeConverter encoder = super.getEncoder(c);

		if (encoder == null && identityConverter.canHandle(c)) {
			encoder = identityConverter;
		}
		return encoder;
	}

	@Override
	protected NoSQLTypeConverter getEncoder(final Object val, final NoSQLDescriptionField descriptionField) {
		if (serializedConverter.canHandle(descriptionField)) {
			return serializedConverter;
		}

		NoSQLTypeConverter encoder = super.getEncoder(val, descriptionField);
		if (encoder == null && (identityConverter.canHandle(descriptionField)
				|| (val != null && identityConverter.isSupported(val.getClass(), descriptionField)))) {
			encoder = identityConverter;
		}
		return encoder;
	}
}
