package com.pg.example.mongodbbatch.mapper;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.LinkedHashMap;
import java.util.Map;

/* Created by Pawel Gawedzki on 27-Mar-18.*/
public class ColumnMapper {

    /**
     * Takes json represented in string and returns map contains name of the column and its type.
     * The result has to be in the same order as it comes from json. That is why LinkedHashMap is used.
     * LinkedHasMap guaranties the order in which elements are put into map.
     * @param json json represented as string
     * @return ordered map where key is name of the column and value is its type
     * @exception JSONException when something is wrong with mapping json on objects
     */
    public static Map<String, String> map(String json) throws JSONException {
        Map<String, String> result = new LinkedHashMap<>();
        JSONObject jsonObject = new JSONObject(json);
        JSONObject foundrySchema = jsonObject.getJSONObject("foundrySchema");
        JSONArray fieldSchemaList = foundrySchema.getJSONArray("fieldSchemaList");

        for (int i = 0; i < fieldSchemaList.length(); i++) {
            JSONObject element = fieldSchemaList.getJSONObject(i);
            result.put(element.getString("name").toUpperCase(), element.getString("type"));
        }

        return result;
    }
}
