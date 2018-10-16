package br.com.anteros.nosql.persistence.mongodb.session;

import com.mongodb.client.result.UpdateResult;

import br.com.anteros.nosql.persistence.session.NoSQLResult;

public class MongoUpdateResult implements NoSQLResult {

	protected long modifiedCount;
	protected UpdateResult updateResult;	
	
	public static MongoUpdateResult of(UpdateResult updateResult) {
		return new MongoUpdateResult(updateResult);
	}
	
	public static MongoUpdateResult of(long modifiedCount) {
		return new MongoUpdateResult(modifiedCount);
	}

	protected MongoUpdateResult(long modifiedCount) {
		this.modifiedCount = modifiedCount;
	}
	
	protected MongoUpdateResult(UpdateResult updateResult) {
		this.updateResult = updateResult;
	}	

	@Override
	public long getModifiedOrDeletedCount() {
		if (updateResult!=null)
			return updateResult.getModifiedCount();
		return modifiedCount;
	}

}
