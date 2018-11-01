package com.pg.example.mongodbbatch.step;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.pg.example.mongodbbatch.data.CreationDetails;
import com.pg.example.mongodbbatch.web.PalantirClient;
import com.pg.example.mongodbbatch.web.QueryColumnBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.stereotype.Component;

import java.util.Date;

/** Created by Pawel Gawedzki on 15-Mar-18.*/
@Component
public class OperationalRouteStep extends AbstractStep {

    private static final String COLLECTION_NAME = "operationalRoutes";
    private static final String DATA_SET_NAME = "operational_routes";
    private static final String DATA_SET_RID = "ri.foundry.main.dataset.4ef1e435-cb2a-450e-ba18-e42263057379";

    @Autowired
    public OperationalRouteStep(MongoTemplate mongoTemplate, PalantirClient palantirClient) {
        super(palantirClient, mongoTemplate, DATA_SET_NAME, COLLECTION_NAME, DATA_SET_RID);
    }

    @Override
    protected void createIndexes() {
        DBCollection tempDBCollection = getTempDBCollection();

        String indexName = "shipment_version_instance_id_1";
        logger.debug("Creating index [{}]", indexName);
        Index index = new Index().on("SHIPMENT_VERSION_INSTANCE_ID", Sort.Direction.DESC)
                .background();
        tempDBCollection.createIndex(index.getIndexKeys(), indexName, false);
    }

    @Override
    protected QueryColumnBuilder getQueryColumnBuilder(String branchName) {
        return QueryColumnBuilder.queryForOperationalRoutesColumns(branchName);
    }

    @Override
    protected CreationDetails createCreationDetail(DBObject dbObject, Date creationDate) {
        return new CreationDetails(
                "SHIPMENT_VERSION_INSTANCE_ID",
                String.valueOf(dbObject.get("SHIPMENT_VERSION_INSTANCE_ID")),
                creationDate
        );
    }
}
