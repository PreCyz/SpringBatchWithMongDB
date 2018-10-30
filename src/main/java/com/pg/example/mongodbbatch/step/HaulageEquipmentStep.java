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

/** Created by Pawel Gawedzki on 16-Mar-18.*/
@Component
public class HaulageEquipmentStep extends AbstractStep {

    private static final String COLLECTION_NAME = "haulageEquipments";
    private static final String DATA_SET_NAME = "haulage_equipment";
    private static final String DATA_SET_RID = "ri.foundry.main.dataset.7fe2b4bc-c60f-4e05-9b36-8f7cd602d5ab";

    @Autowired
    public HaulageEquipmentStep(MongoTemplate mongoTemplate, PalantirClient palantirClient) {
        super(palantirClient, mongoTemplate, DATA_SET_NAME, COLLECTION_NAME, DATA_SET_RID);
    }

    @Override
    protected void createIndexes() {
        DBCollection tempDBCollection = getTempDBCollection();
        /*Index indexCode = new Index().on("RKST_CODE", Sort.Direction.ASC);
        tempDBCollection.createIndex(indexCode.getIndexKeys(),"RKST_CODE",true);

        Index indexCountry = new Index().on("COUNTRY_RKST_CODE", Sort.Direction.ASC);
        tempDBCollection.createIndex(indexCountry.getIndexKeys(),"COUNTRY_RKST_CODE",false);

        Index indexLocationType = new Index().on("LOCATION_TYPE", Sort.Direction.ASC);
        tempDBCollection.createIndex(indexLocationType.getIndexKeys(),"LOCATION_TYPE",false);*/
    }

    @Override
    protected QueryColumnBuilder getQueryColumnBuilder(String branchName) {
        return QueryColumnBuilder.queryForHaulageEquipmentColumns(branchName);
    }

    @Override
    protected CreationDetails createCreationDetail(DBObject dbObject, Date creationDate) {
        return null;
    }
}
