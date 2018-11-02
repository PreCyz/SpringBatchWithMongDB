package com.pg.example.mongodbbatch.step;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.pg.example.mongodbbatch.data.CreationDetails;
import com.pg.example.mongodbbatch.mapper.ColumnMapper;
import com.pg.example.mongodbbatch.util.TimeLog;
import com.pg.example.mongodbbatch.web.PalantirClient;
import com.pg.example.mongodbbatch.web.QueryColumnBuilder;
import com.pg.example.mongodbbatch.web.ResultType;
import com.pg.example.mongodbbatch.web.UrlBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.*;
import java.util.*;

/**Created by Pawel Gawedzki on 15-Mar-18.*/
abstract class AbstractStep implements Tasklet {

    protected final Logger logger;
    private static final String ERROR_COLLECTION_NAME = "csvErrors";

    @Value("${palantir.endpoint}")
    private String basicUrl;
    @Value("${mongodb.bulk.batchSize}")
    private int batchSize = 1000;
    @Value("${csv.chunkSize}")
    private int chunkSize = 50000;
    @Value("${palantir.branch}")
    private String branchName;
    @Value("${csv.directory}")
    private String csvDirectory = "./csv";

    private MongoTemplate mongoTemplate;
    private final PalantirClient palantirClient;
    private final String dataSetName;
    private final String collectionName;
    private final String dataSetRid;
    private String[] headers;
    private final String filePath;
    private final Collection<CreationDetails> creationDetails;

    protected AbstractStep(PalantirClient palantirClient, MongoTemplate mongoTemplate, String dataSetName,
                           String collectionName, String dataSetRid) {
        this.palantirClient = palantirClient;
        this.mongoTemplate = mongoTemplate;
        this.dataSetName = dataSetName;
        this.collectionName = collectionName;
        this.dataSetRid = dataSetRid;
        this.filePath = String.format("%s/%s.csv", csvDirectory, dataSetName);
        this.creationDetails = new HashSet<>();
        this.logger = LoggerFactory.getLogger(this.getClass());
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws IOException, JSONException {
        TimeLog timeLog = new TimeLog(getClass().getSimpleName());

        timeLog.logTime("dropTemporaryCollection");
        dropTemporaryCollection();

        timeLog.logTime("extractColumns");
        extractColumns();

        timeLog.logTime("downloadCsv");
        downloadCsv();

        timeLog.logTime("mapAndSaveToDB");
        mapAndSaveToDB();

        timeLog.logTime("createIndexes");
        createIndexes();

        timeLog.logTime("renameTempCollection");
        renameTempCollection();

        timeLog.logTime("upsertCreationDetails");
        upsertCreationDetails();

        timeLog.logTime("deleteFile");
        deleteFile();

        timeLog.done();
        return RepeatStatus.FINISHED;
    }

    private void renameTempCollection() {
        logger.debug("Renaming '{}' collection to '{}'", getTempCollectionName(), collectionName);
        DBCollection tempDBCollection = getTempDBCollection();
        tempDBCollection.rename(collectionName, true);
    }

    private void downloadCsv() throws IOException {
        logger.debug("Requesting Palantir for data set [{}.{}].", branchName, dataSetName);
        palantirClient.downloadDataToFile(urlBuilder(), filePath);
    }

    private UrlBuilder urlBuilder() {
        return new UrlBuilder()
                .basicUrl(basicUrl + "/datasets")
                .branchName(branchName)
                .dataSetName(dataSetName)
                .dataSetRid(dataSetRid)
                .resultType(ResultType.csv);
    }

    private void mapAndSaveToDB() throws IOException {
        List<DBObject> objectsToSave = new ArrayList<>(chunkSize);
        List<DBObject> errorObjects = new LinkedList<>();
        int numberOfDocuments = 0;
        int chunkNumber = 0;

        try (
                InputStream inputStream = new FileInputStream(filePath);
                Reader inputStreamReader = new InputStreamReader(inputStream);
                final CSVParser parser = new CSVParser(inputStreamReader, CSVFormat.DEFAULT)
        ) {
            for (CSVRecord record : parser) {
                if (record.size() != headers.length) {
                    List<String> list = new ArrayList<>();
                    record.iterator().forEachRemaining(list::add);
                    logger.error("Wrong column mapping. There are [values, headers] == <{}, {}>. Row with error parsing [{}].",
                            record.size(),
                            headers.length,
                            String.join("&&&", list),
                            new IllegalArgumentException("Wrong column mapping.")
                    );
                    errorObjects.add(createErrorObject(record));
                    continue;
                }

                objectsToSave.add(createDBObject(record));

                if (objectsToSave.size() == chunkSize) {
                    chunkNumber++;
                    logger.debug("Saving chunk number {}.", chunkNumber);
                    saveIntoCollection(objectsToSave, getTempCollectionName());
                    numberOfDocuments += objectsToSave.size();
                    objectsToSave.clear();
                }
            }
            if (!objectsToSave.isEmpty()) {
                saveIntoCollection(objectsToSave, getTempCollectionName());
                numberOfDocuments += objectsToSave.size();
            }
            if (!errorObjects.isEmpty()) {
                saveIntoCollection(errorObjects, ERROR_COLLECTION_NAME);
            }
        }

        logger.debug("{} documents saved. {} error documents saved.", numberOfDocuments, errorObjects.size());
    }

    private DBObject createErrorObject(CSVRecord record) {
        DBObject errorObject = new BasicDBObject();
        for (int i = 0; i < record.size(); i++) {
            if (i < headers.length) {
                errorObject.put(headers[i], record.get(i));
            } else {
                errorObject.put(String.format("UNKNOWN_HEADER_%d", i), record.get(i));
            }
        }
        errorObject.put("DATA_SET_NAME", dataSetName);
        errorObject.put("CREATED", new Date(System.currentTimeMillis()));

        return errorObject;
    }

    private DBObject createDBObject(CSVRecord record) {
        DBObject dbObject = new BasicDBObject();
        for (int i = 0; i < headers.length; i++) {
            dbObject.put(headers[i], record.get(i));
        }
        addAdditionalData(dbObject);
        return dbObject;
    }

    private void addAdditionalData(DBObject dbObject) {
        Date creationDate = new Date(System.currentTimeMillis());
        dbObject.put("CREATED", creationDate);
        creationDetails.add(createCreationDetail(dbObject, creationDate));
    }

    private void dropTemporaryCollection() {
        String tempCollectionName = getTempCollectionName();
        if (mongoTemplate.collectionExists(tempCollectionName)) {
            mongoTemplate.dropCollection(tempCollectionName);
        }
    }

    private String getTempCollectionName() {
        return String.format("TMP-%s", collectionName);
    }

    private void extractColumns() throws JSONException {
        logger.debug("Requesting Palantir for data set columns [{}.{}].", branchName, dataSetName);
        String jsonResponse = palantirClient.getSchemaData(getQueryColumnBuilder(branchName), basicUrl + "/query");
        Map<String, String> nameTypeMap = ColumnMapper.map(jsonResponse);
        logger.debug("[{}.{}] has {} columns as follows {}.", branchName, dataSetName, nameTypeMap.size(), nameTypeMap);
        headers = new String[nameTypeMap.keySet().size()];
        headers = nameTypeMap.keySet().toArray(headers);
    }

    private void saveIntoCollection(List<DBObject> elements, String collectionName) {
        logger.debug("Inserting [{}] into collection [{}]", dataSetName, collectionName);

        int numberOfExecution = 0;
        int totalDocumentInserted = 0;
        int totalDocumentsProcessed = 0;

        BulkOperations bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, collectionName);
        for (DBObject element : elements) {
            bulkOps.insert(element);
            totalDocumentsProcessed++;
            if (totalDocumentsProcessed % batchSize == 0) {
                numberOfExecution++;
                totalDocumentInserted += executeBulk(bulkOps, numberOfExecution, totalDocumentsProcessed, totalDocumentInserted);
                bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, collectionName);
            }
        }
        //if there is more but less than batch size we need to execute batch
        if (totalDocumentsProcessed % batchSize != 0) {
            numberOfExecution++;
            executeBulk(bulkOps, numberOfExecution, elements.size(), totalDocumentInserted);
        }
    }

    private int executeBulk(BulkOperations bulkOps, int numberOfExecutions, int totalDocumentsProcessed, int totalDocumentsInserted) {
        BulkWriteResult result = bulkOps.execute();
        logger.debug("{} bulk execution(s). Documents [processed, inserted] = <{}, {}>",
                numberOfExecutions, totalDocumentsProcessed, totalDocumentsInserted + result.getInsertedCount());
        return result.getInsertedCount();
    }

    final DBCollection getTempDBCollection() {
        return mongoTemplate.getCollection(getTempCollectionName());
    }

    private void deleteFile() throws IOException {
        logger.debug("Deleting file [{}]", filePath);
        FileUtils.forceDelete(new File(filePath));
    }

    private void upsertCreationDetails() {
        if (!creationDetails.isEmpty()) {

            int numberOfExecution = 0;
            int totalDocumentInserted = 0;
            int totalDocumentsProcessed = 0;

            BulkOperations bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, CreationDetails.COLLECTION_NAME);
            for (CreationDetails detail : creationDetails) {
                Query query = new Query();
                query.addCriteria(Criteria.where("name").is(detail.getName()));
                query.addCriteria(Criteria.where("value").is(detail.getValue()));

                Update upsert = new Update();

                upsert.set("lastUpdate", detail.getCreated());

                upsert.setOnInsert("name", detail.getName());
                upsert.setOnInsert("value", detail.getValue());
                upsert.setOnInsert("created", detail.getCreated());

                bulkOps.upsert(query, upsert);

                totalDocumentsProcessed++;
                if (totalDocumentsProcessed % batchSize == 0) {
                    numberOfExecution++;
                    totalDocumentInserted += executeBulk(bulkOps, numberOfExecution, totalDocumentsProcessed, totalDocumentInserted);
                    bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, CreationDetails.COLLECTION_NAME);
                }
            }
            //if there is more but less than batch size we need to execute batch
            if (totalDocumentsProcessed % batchSize != 0) {
                numberOfExecution++;
                executeBulk(bulkOps, numberOfExecution, creationDetails.size(), totalDocumentInserted);
            }
        }
    }

    protected abstract void createIndexes();
    protected abstract QueryColumnBuilder getQueryColumnBuilder(String branchName);
    protected abstract CreationDetails createCreationDetail(DBObject dbObject, Date creationDate);
}
