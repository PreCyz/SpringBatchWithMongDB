package com.pg.example.mongodbbatch.step;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.pg.example.mongodbbatch.data.CreationDetails;
import com.pg.example.mongodbbatch.web.PalantirClient;
import com.pg.example.mongodbbatch.web.QueryColumnBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.stereotype.Component;

import java.util.Date;

/** Created by Pawel Gawedzki on 16-Mar-18.*/
@Component
public class HaulageInfoStep extends AbstractStep {

    private static final String COLLECTION_NAME = "haulageInfo";
    private static final String DATA_SET_NAME = "haulage_info";
    private static final String DATA_SET_RID = "ri.foundry.main.dataset.49252e4a-2697-436a-876f-cf73c28d90b9";

    @Autowired
    public HaulageInfoStep(MongoTemplate mongoTemplate, PalantirClient palantirClient) {
        super(palantirClient, mongoTemplate, DATA_SET_NAME, COLLECTION_NAME, DATA_SET_RID);
    }

    @Override
    protected void createIndexes() {
        DBCollection tempDBCollection = getTempDBCollection();

        String indexName = "fk_shipment_version_imp_exp_1_direction_1";
        logger.debug("Creating index [{}]", indexName);
        DBObject endLoc = new BasicDBObject();
        endLoc.put("FK_SHIPMENT_VERSION_IMP_EXP", 1);
        endLoc.put("DIRECTION", 1);
        Index endLocIndex = new CompoundIndexDefinition(endLoc).named(indexName).background();
        tempDBCollection.createIndex(endLocIndex.getIndexKeys(), endLocIndex.getIndexOptions());
    }

    @Override
    protected QueryColumnBuilder getQueryColumnBuilder(String branchName) {
        return QueryColumnBuilder.queryForHaulageInfoColumns(branchName);
    }

    @Override
    protected CreationDetails createCreationDetail(DBObject dbObject, Date creationDate) {
        return new CreationDetails("FK_SHIPMENT_VERSION_IMP_EXP", String.valueOf(dbObject.get("FK_SHIPMENT_VERSION_IMP_EXP")), creationDate);
    }
}
