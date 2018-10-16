package br.com.anteros.nosql.persistence.mongodb.client;

import java.util.Properties;

import br.com.anteros.nosql.persistence.client.NoSQLDataSource;
import br.com.anteros.nosql.persistence.client.NoSQLDataSourceBuilder;
import br.com.anteros.nosql.persistence.session.configuration.DataSourceConfiguration;
import br.com.anteros.nosql.persistence.session.configuration.PropertiesConfiguration;


public class MongoDataSourceBuilder implements NoSQLDataSourceBuilder {
	
	private DataSourceConfiguration configuration;
	private Properties props;
	private PropertiesConfiguration propsConf;

	@Override
	public NoSQLDataSourceBuilder configure(DataSourceConfiguration configuration) {
		this.configuration = configuration;
		return this;
	}

	@Override
	public NoSQLDataSourceBuilder configure(Properties props) {
		this.props = props;
		return this;
	}

	@Override
	public NoSQLDataSource build() {
		return new MongoDataSource(configuration,props, propsConf);
	}

	@Override
	public NoSQLDataSourceBuilder configure(PropertiesConfiguration props) {
		this.propsConf = props;
		return this;
	}


}
