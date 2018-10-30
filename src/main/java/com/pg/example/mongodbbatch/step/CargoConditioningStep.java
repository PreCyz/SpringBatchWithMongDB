package com.pg.example.mongodbbatch.step;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.pg.example.mongodbbatch.data.CreationDetails;
import com.pg.example.mongodbbatch.web.PalantirClient;
import com.pg.example.mongodbbatch.web.QueryColumnBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;

/** Created by Pawel Gawedzki on 15-Mar-18.*/
@Component
public class CargoConditioningStep extends AbstractStep {

    private static final String COLLECTION_NAME = "cargoConditioning";
    private static final String DATA_SET_NAME = "cargo_conditioning";
    private static final String DATA_SET_RID = "ri.foundry.main.dataset.2c1a086a-afa3-4e74-a382-88edc571056c";

    @Autowired
    public CargoConditioningStep(MongoTemplate mongoTemplate, PalantirClient palantirClient) {
        super(palantirClient, mongoTemplate, DATA_SET_NAME, COLLECTION_NAME, DATA_SET_RID);
    }

    @Override
    protected void createIndexes() {
        DBCollection tempDBCollection = getTempDBCollection();

        /*String indexName = "endLoc_1_depTimeExp_1";
        logger.debug("Creating index [{}]", indexName);
        DBObject endLoc = new BasicDBObject();
        endLoc.put("END_LOC", 1);
        endLoc.put("DEP_TIME_EXP", 1);
        Index endLocIndex = new CompoundIndexDefinition(endLoc).named(indexName).background();
        tempDBCollection.createIndex(endLocIndex.getIndexKeys(), endLocIndex.getIndexOptions());

        indexName = "startLoc_1_depTimeExp_1";
        logger.debug("Creating index [{}]", indexName);
        DBObject startLoc = new BasicDBObject();
        startLoc.put("START_LOC", 1);
        startLoc.put("DEP_TIME_EXP", 1);
        Index startLocIndex = new CompoundIndexDefinition(startLoc).named(indexName).background();
        tempDBCollection.createIndex(startLocIndex.getIndexKeys(), startLocIndex.getIndexOptions());

        indexName = "booking_number_1";
        logger.debug("Creating index [{}]", indexName);
        Index index = new Index().on("BOOKING_NUMBER", Sort.Direction.DESC).background();
        tempDBCollection.createIndex(index.getIndexKeys(), indexName, false);*/
    }

    @Override
    protected QueryColumnBuilder getQueryColumnBuilder(String branchName) {
        return QueryColumnBuilder.queryForGlobalBookingsTruckinglegsColumns(branchName);
    }

    @Override
    protected CreationDetails createCreationDetail(DBObject dbObject, Date creationDate) {
        return null;
    }
}
