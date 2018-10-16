package br.com.anteros.nosql.persistence.mongodb.mapping;


import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;


import br.com.anteros.core.log.LogLevel;
import br.com.anteros.core.log.Logger;
import br.com.anteros.core.log.LoggerProvider;
import br.com.anteros.nosql.persistence.converters.NoSQLMappingException;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;
import br.com.anteros.nosql.persistence.session.mapping.NoSQLMapperOptions;
import br.com.anteros.nosql.persistence.session.mapping.NoSQLObjectFactory;



public class DefaultMongoCreator implements NoSQLObjectFactory {

	private static Logger LOG = LoggerProvider.getInstance().getLogger(NoSQLObjectFactory.class);

    private Map<String, Class<?>> classNameCache = new ConcurrentHashMap<>();

    private NoSQLMapperOptions options = null;


    public DefaultMongoCreator() {
    }


    public DefaultMongoCreator(final NoSQLMapperOptions options) {
        this.options = options;
    }

    private static <T> Constructor<T> getNoArgsConstructor(final Class<T> type) {
        try {
            final Constructor<T> constructor = type.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor;
        } catch (NoSuchMethodException e) {
            throw new NoSQLMappingException("No usable constructor for " + type.getName(), e);
        }
    }



    @Override
    @SuppressWarnings("unchecked")
    public <T> T createInstance(final Class<T> clazz) {
        try {
            return getNoArgsConstructor(clazz).newInstance();
        } catch (Exception e) {
            if (Collection.class.isAssignableFrom(clazz)) {
                return (T) createList(null);
            } else if (Map.class.isAssignableFrom(clazz)) {
                return (T) createMap(null);
            } else if (Set.class.isAssignableFrom(clazz)) {
                return (T) createSet(null);
            }
            throw new NoSQLMappingException("No usable constructor for " + clazz.getName(), e);
        }
    }

    @Override
    public <T> T createInstance(final Class<T> clazz, final Object dbObj) {
        Class<T> c = getClass((Document)dbObj);
        if (c == null) {
            c = clazz;
        }
        return createInstance(c);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object createInstance(final AbstractNoSQLObjectMapper mapper, final NoSQLDescriptionField descriptionField, final Object dbObj) {
        Class<?> c = getClass((Document)dbObj);
        if (c == null) {
            c = descriptionField.isSimple() ? descriptionField.getConcreteType() : descriptionField.getSubClass();
            if (c.equals(Object.class)) {
                c = descriptionField.getConcreteType();
            }
        }
        try {
            return createInstance(c, dbObj);
        } catch (RuntimeException e) {
            if (!descriptionField.hasConstructorArgs()) {
                throw e;
            }
            final Object[] args = new Object[descriptionField.getConstructorArgs().length];
            final Class<?>[] argTypes = new Class[descriptionField.getConstructorArgs().length];
            for (int i = 0; i < descriptionField.getConstructorArgs().length; i++) {
                final Object val = ((Document)dbObj).get(descriptionField.getConstructorArgs()[i]);
                args[i] = val;
                argTypes[i] = val.getClass();
            }
            try {
                final Constructor<?> constructor = c.getDeclaredConstructor(argTypes);
                constructor.setAccessible(true);
                return constructor.newInstance(args);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<?> createList(final NoSQLDescriptionField descriptionField) {
        return newInstance(descriptionField != null ? descriptionField.getConstructor() : null, ArrayList.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<?,?> createMap(final NoSQLDescriptionField descriptionField) {
        return newInstance(descriptionField != null ? descriptionField.getConstructor() : null, HashMap.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<?> createSet(final NoSQLDescriptionField descriptionField) {
        return newInstance(descriptionField != null ? descriptionField.getConstructor() : null, HashSet.class);
    }

   
    public Map<String, Class<?>> getClassNameCache() {
        HashMap<String, Class<?>> copy = new HashMap<String, Class<?>>();
        copy.putAll(classNameCache);
        return copy;
    }

    protected ClassLoader getClassLoaderForClass() {
        return Thread.currentThread().getContextClassLoader();
    }

    private <T> Class<T> getClass(final Document dbObj) {
        Class c = null;
        if (dbObj.containsKey(AbstractNoSQLObjectMapper.CLASS_NAME_FIELDNAME)) {
            final String className = (String) dbObj.get(AbstractNoSQLObjectMapper.CLASS_NAME_FIELDNAME);
            try {
                if (options != null && options.isCacheClassLookups()) {
                    c = classNameCache.get(className);
                    if (c == null) {
                        c = Class.forName(className, true, getClassLoaderForClass());
                        classNameCache.put(className, c);
                    }
                } else {
                    c = Class.forName(className, true, getClassLoaderForClass());
                }
            } catch (ClassNotFoundException e) {
                if (LOG.isWarnEnabled()) {
                    LOG.log(LogLevel.WARN,"Class not found defined in dbObj: ", e);
                }
            }
        }
        return c;
    }

  
    private <T> T newInstance(final Constructor<T> tryMe, final Class<T> fallbackType) {
        if (tryMe != null) {
            tryMe.setAccessible(true);
            try {
                return tryMe.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return createInstance(fallbackType);
    }

}
