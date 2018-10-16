package br.com.anteros.nosql.persistence.mongodb.session;

import com.mongodb.client.result.DeleteResult;

import br.com.anteros.nosql.persistence.session.NoSQLResult;

public class MongoDeleteResult implements NoSQLResult {

	protected long deletedCount;
	protected DeleteResult deleteResult;

	public static MongoDeleteResult of(DeleteResult deleteResult) {
		return new MongoDeleteResult(deleteResult);
	}

	public static MongoDeleteResult of(long deletedCount) {
		return new MongoDeleteResult(deletedCount);
	}

	protected MongoDeleteResult(long deletedCount) {
		this.deletedCount = deletedCount;
	}

	protected MongoDeleteResult(DeleteResult deleteResult) {
		this.deleteResult = deleteResult;
	}

	@Override
	public long getModifiedOrDeletedCount() {
		if (deleteResult != null)
			return deleteResult.getDeletedCount();
		return deletedCount;
	}

}
