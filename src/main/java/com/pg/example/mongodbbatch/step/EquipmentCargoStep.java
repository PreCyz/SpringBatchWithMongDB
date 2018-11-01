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
public class EquipmentCargoStep extends AbstractStep {

    private static final String COLLECTION_NAME = "equipmentCargo";
    private static final String DATA_SET_NAME = "equipment_cargo";
    private static final String DATA_SET_RID = "ri.foundry.main.dataset.0fc9d55a-142e-4385-883d-db1c1a5ef2b4";

    @Autowired
    public EquipmentCargoStep(MongoTemplate mongoTemplate, PalantirClient palantirClient) {
        super(palantirClient, mongoTemplate, DATA_SET_NAME, COLLECTION_NAME, DATA_SET_RID);
    }

    @Override
    protected void createIndexes() {
        DBCollection tempDBCollection = getTempDBCollection();

        String indexName = "fk_shipment_version_1";
        logger.debug("Creating index [{}]", indexName);
        Index index = new Index().on("FK_SHIPMENT_VERSION", Sort.Direction.DESC)
                .background();
        tempDBCollection.createIndex(index.getIndexKeys(), indexName, false);

        indexName = "equipment_assignment_instance_id_1";
        logger.debug("Creating index [{}]", indexName);
        index = new Index().on("EQUIPMENT_ASSIGNMENT_INSTANCE_ID", Sort.Direction.DESC)
                .background();
        tempDBCollection.createIndex(index.getIndexKeys(), indexName, false);
    }

    @Override
    protected QueryColumnBuilder getQueryColumnBuilder(String branchName) {
        return QueryColumnBuilder.queryForEquipmentCargoColumns(branchName);
    }

    @Override
    protected CreationDetails createCreationDetail(DBObject dbObject, Date creationDate) {
        return new CreationDetails(
                "EQUIPMENT_ASSIGNMENT_INSTANCE_ID",
                String.valueOf(dbObject.get("EQUIPMENT_ASSIGNMENT_INSTANCE_ID")),
                creationDate
        );
    }
}
