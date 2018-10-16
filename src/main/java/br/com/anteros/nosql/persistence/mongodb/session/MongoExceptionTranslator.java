package br.com.anteros.nosql.persistence.mongodb.session;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.bson.BsonInvalidOperationException;

import com.mongodb.BulkWriteException;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoServerException;
import com.mongodb.bulk.BulkWriteError;

import br.com.anteros.core.utils.ClassUtils;
import br.com.anteros.nosql.persistence.mongodb.query.InvalidDataAccessApiUsageException;
import br.com.anteros.nosql.persistence.mongodb.utils.MongoDbErrorCodes;
import br.com.anteros.nosql.persistence.session.exception.NoSQLDataAccessException;
import br.com.anteros.nosql.persistence.session.exception.NoSQLExceptionTranslator;

public class MongoExceptionTranslator implements NoSQLExceptionTranslator {

	private static final Set<String> DUPLICATE_KEY_EXCEPTIONS = new HashSet<>(
			Arrays.asList("MongoException.DuplicateKey", "DuplicateKeyException"));

	private static final Set<String> RESOURCE_FAILURE_EXCEPTIONS = new HashSet<>(
			Arrays.asList("MongoException.Network", "MongoSocketException", "MongoException.CursorNotFound",
					"MongoCursorNotFoundException", "MongoServerSelectionException", "MongoTimeoutException"));

	private static final Set<String> RESOURCE_USAGE_EXCEPTIONS = new HashSet<>(
			Collections.singletonList("MongoInternalException"));

	private static final Set<String> DATA_INTEGRITY_EXCEPTIONS = new HashSet<>(
			Arrays.asList("WriteConcernException", "MongoWriteException", "MongoBulkWriteException"));

	public NoSQLDataAccessException translateExceptionIfPossible(Exception ex) {

		// Check for well-known MongoException subclasses.

		if (ex instanceof BsonInvalidOperationException) {
			throw new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
		}

		String exception = ClassUtils.getShortName(ClassUtils.getUserClass(ex.getClass()));

		if (DUPLICATE_KEY_EXCEPTIONS.contains(exception)) {
			return new DuplicateKeyException(ex.getMessage(), ex);
		}

		if (RESOURCE_FAILURE_EXCEPTIONS.contains(exception)) {
			return new DataAccessResourceFailureException(ex.getMessage(), ex);
		}

		if (RESOURCE_USAGE_EXCEPTIONS.contains(exception)) {
			return new InvalidDataAccessResourceUsageException(ex.getMessage(), ex);
		}

		if (DATA_INTEGRITY_EXCEPTIONS.contains(exception)) {

			if (ex instanceof MongoServerException) {
				if (((MongoServerException) ex).getCode() == 11000) {
					return new DuplicateKeyException(ex.getMessage(), ex);
				}
				if (ex instanceof MongoBulkWriteException) {
					for (BulkWriteError x : ((MongoBulkWriteException) ex).getWriteErrors()) {
						if (x.getCode() == 11000) {
							return new DuplicateKeyException(ex.getMessage(), ex);
						}
					}
				}
			}

			return new DataIntegrityViolationException(ex.getMessage(), ex);
		}

		if (ex instanceof BulkWriteException) {
			return new BulkOperationException(ex.getMessage(), (BulkWriteException) ex);
		}

		if (ex instanceof MongoException) {

			int code = ((MongoException) ex).getCode();

			if (MongoDbErrorCodes.isDuplicateKeyCode(code)) {
				return new DuplicateKeyException(ex.getMessage(), ex);
			} else if (MongoDbErrorCodes.isDataAccessResourceFailureCode(code)) {
				return new DataAccessResourceFailureException(ex.getMessage(), ex);
			} else if (MongoDbErrorCodes.isInvalidDataAccessApiUsageCode(code) || code == 10003 || code == 12001
					|| code == 12010 || code == 12011 || code == 12012) {
				return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
			} else if (MongoDbErrorCodes.isPermissionDeniedCode(code)) {
				return new PermissionDeniedDataAccessException(ex.getMessage(), ex);
			} else if (MongoDbErrorCodes.isClientSessionFailureCode(code)) {
				return new ClientSessionException(ex.getMessage(), ex);
			} else if (MongoDbErrorCodes.isTransactionFailureCode(code)) {
				return new MongoTransactionException(ex.getMessage(), ex);
			}
			return new UncategorizedMongoDbException(ex.getMessage(), ex);
		}

		if (ex instanceof IllegalStateException) {
			for (StackTraceElement elm : ex.getStackTrace()) {
				if (elm.getClassName().contains("ClientSession")) {
					return new ClientSessionException(ex.getMessage(), ex);
				}
			}
		}

		return null;
	}
	
	
}
