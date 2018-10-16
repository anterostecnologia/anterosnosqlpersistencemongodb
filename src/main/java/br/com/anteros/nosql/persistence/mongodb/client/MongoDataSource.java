package br.com.anteros.nosql.persistence.mongodb.client;

import java.util.Arrays;
import java.util.Properties;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import br.com.anteros.core.utils.StringUtils;
import br.com.anteros.nosql.persistence.client.NoSQLConnection;
import br.com.anteros.nosql.persistence.client.NoSQLDataSource;
import br.com.anteros.nosql.persistence.metadata.configuration.AnterosNoSQLProperties;
import br.com.anteros.nosql.persistence.mongodb.session.MongoExceptionTranslator;
import br.com.anteros.nosql.persistence.session.configuration.DataSourceConfiguration;
import br.com.anteros.nosql.persistence.session.configuration.PropertiesConfiguration;


public class MongoDataSource implements NoSQLDataSource {

	private MongoClient client;
	private String databaseName;
	private String password;
	private String userName;
	private String port;
	private String host;

	public MongoDataSource(DataSourceConfiguration configuration, Properties props, PropertiesConfiguration propsConf) {
		if (props != null) {
			host = props.getProperty(AnterosNoSQLProperties.CONNECTION_HOST);
			port = props.getProperty(AnterosNoSQLProperties.CONNECTION_PORT);
			userName = props.getProperty(AnterosNoSQLProperties.CONNECTION_USER);
			password = props.getProperty(AnterosNoSQLProperties.CONNECTION_PASSWORD);
			databaseName = props.getProperty(AnterosNoSQLProperties.DATABASE_NAME);

			client = MongoClients.create(MongoClientSettings.builder()
					.credential(MongoCredential.createCredential(userName, databaseName, StringUtils.isNotBlank(password)?password.toCharArray():null))
					.applyToClusterSettings(
							builder -> builder.hosts(Arrays.asList(new ServerAddress(host, Integer.parseInt(port)))))
					.build());

		} else if (propsConf != null) {
			host = propsConf.getProperty(AnterosNoSQLProperties.CONNECTION_HOST);
			port = propsConf.getProperty(AnterosNoSQLProperties.CONNECTION_PORT);
			userName = propsConf.getProperty(AnterosNoSQLProperties.CONNECTION_USER);
			password = propsConf.getProperty(AnterosNoSQLProperties.CONNECTION_PASSWORD);
			databaseName = propsConf.getProperty(AnterosNoSQLProperties.DATABASE_NAME);
			if (StringUtils.isNotBlank(userName)) {
			  client = MongoClients.create(MongoClientSettings.builder()
						.credential(MongoCredential.createCredential(userName, databaseName, StringUtils.isNotBlank(password)?password.toCharArray():null))
						.applyToClusterSettings(
								builder -> builder.hosts(Arrays.asList(new ServerAddress(host, Integer.parseInt(port)))))
						.build());
			} else {
				client = MongoClients.create(MongoClientSettings.builder()
						.applyToClusterSettings(
								builder -> builder.hosts(Arrays.asList(new ServerAddress(host, Integer.parseInt(port)))))
						.build());
			}
		}
	}

	@Override
	public NoSQLConnection getConnection() {
		return new MongoConnection(client, databaseName, true, new MongoExceptionTranslator());
	}

}
