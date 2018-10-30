package com.pg.example.mongodbbatch.config;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;

/** Created by Pawel Gawedzki on 15-Mar-18.*/
@Configuration
public class MongoDBConfig extends AbstractMongoConfiguration {

    private static Logger logger = LoggerFactory.getLogger(MongoDBConfig.class);

    @Value("${spring.data.mongodb.database}")
    private String database;

    @Value("${spring.data.mongodb.host}")
    private String host;

    @Override
    protected String getDatabaseName() {
        return database;
    }

    @Override
    public MongoClient mongo() {
        MongoClientOptions.Builder mongoClientOptionsBuilder = MongoClientOptions.builder()
                .writeConcern(WriteConcern.ACKNOWLEDGED)
                .socketKeepAlive(true);
        return new MongoClient(new MongoClientURI(host, mongoClientOptionsBuilder));
    }
}
