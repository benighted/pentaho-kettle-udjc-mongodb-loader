import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import java.util.Map;
import java.util.Iterator;

private MongoClient mongo;
private DB db;
private DBCollection coll;
private JSONParser parser;
private String[] fieldNames;

private final String emptyString = "";
private final String idFieldName = "_id";
private final String objSuffix = "_obj";
private final String arrSuffix = "_arr";
private final String arrIntSuffix = "_int_arr";
private final String arrObjSuffix = "_obj_arr";

public boolean init(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface) {
    int dbPort = 27017;
    String dbHost = "localhost";
    String dbName = "test";
    String collName = "product";

    try {
        mongo = new MongoClient(dbHost, dbPort);
        db = mongo.getDB(dbName);
        coll = db.getCollection(collName);
        parser = new JSONParser();

        coll.remove(new BasicDBObject()); // to truncate collection
        
        logMinimal("Connected to database " + dbName + "." + collName);
    } catch (Exception e) {
        logError("Error connecting to database: ", e);
        return false;
    }

    return parent.initImpl(stepMetaInterface, stepDataInterface);
}

public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
    Object[] row = getRow();

    if (first) {
        fieldNames = data.inputRowMeta.getFieldNames();
        first = false;
    }

    if (row == null) {
        setOutputDone();
        return false;
    }

    coll.save(getDocument(row));

    putRow(data.outputRowMeta, row);

    return true;
}

private BasicDBObject getDocument(Object[] row) throws KettleException {
    BasicDBObject doc = new BasicDBObject();
    for (int i = 0; i < fieldNames.length; i++) {
        if (idFieldName.equals(fieldNames[i]))  {
            doc.append(idFieldName, get(Fields.In, idFieldName).getInteger(row).intValue());
        } else if (fieldNames[i].endsWith(arrSuffix)) {
            if (fieldNames[i].endsWith(arrIntSuffix)) {
              doc.append(fieldNames[i].replace(arrIntSuffix,emptyString), parseList(get(Fields.In, fieldNames[i]).getString(row),"int"));
            } else if (fieldNames[i].endsWith(arrObjSuffix)) {
              doc.append(fieldNames[i].replace(arrObjSuffix,emptyString), parseList(get(Fields.In, fieldNames[i]).getString(row),"obj"));
            } else {
              doc.append(fieldNames[i].replace(arrSuffix,emptyString), parseList(get(Fields.In, fieldNames[i]).getString(row)));
            }
        } else if (fieldNames[i].endsWith(objSuffix)) {
            doc.append(fieldNames[i].replace(objSuffix,emptyString), parseObject(get(Fields.In, fieldNames[i]).getString(row)));
        } else {
            doc.append(fieldNames[i], get(Fields.In, fieldNames[i]).getString(row));
        }
    }

    return doc;
}

private BasicDBList parseList(String value) {
    return parseList(value, "string");
}

private BasicDBList parseList(String value, String dataType) {
    BasicDBList dbList = new BasicDBList();
    JSONArray array = null;

    if (value != null) {
        try {
            array = (JSONArray) parser.parse(value);
        } catch (Exception e) {
            logError("Error parsing JSON array: " + value, e);
            stopAll();
        }

        for(int i = 0; i < array.size(); i++) {
            if (dataType.equals("int") || dataType.equals("integer")) {
                try {
                    dbList.put(i, Integer.parseInt(array.get(i).toString()));
                } catch (Exception e) {
                    logError("Error parsing integer: " + array.get(i).toString(), e);
                    stopAll();
                }
            } else if (dataType.equals("obj") || dataType.equals("object")) {
                try {
                    dbList.put(i, parseObject(array.get(i).toString()));
                } catch (Exception e) {
                    logError("Error parsing object: " + array.get(i).toString(), e);
                    stopAll();
                }
            } else {
                dbList.put(i, array.get(i).toString());
            }
        }
    }

    return dbList;
}

private BasicDBObject parseObject(String value) {
    BasicDBObject dbObj = new BasicDBObject();
    JSONObject obj = null;

    if (value != null) {
        try {
            obj = (JSONObject) parser.parse(value);
            Iterator it = obj.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry entry = (Map.Entry) it.next();
                if (idFieldName.equals(entry.getKey().toString()))  {
                    dbObj.append(idFieldName, Integer.parseInt(entry.getValue().toString()));
                } else {
                    dbObj.append(entry.getKey().toString(), entry.getValue());
                }
            }
        } catch (Exception e) {
            logError("Error parsing JSON object: " + value, e);
            stopAll();
            return null;
        }
    }

    return dbObj;
}