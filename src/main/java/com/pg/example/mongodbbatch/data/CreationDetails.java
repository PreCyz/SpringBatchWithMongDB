package com.pg.example.mongodbbatch.data;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/* Created by Pawel Gawedzki on 19-Apr-18.*/
@Document(collection = CreationDetails.COLLECTION_NAME)
@CompoundIndex(name = "name_1_value_1", def = "{'name' : 1, 'value' : 1}")
public class CreationDetails {

    public final static String COLLECTION_NAME = "creationDetails";

    @Id
    private String id;
    @Indexed
    private String name;
    private String value;
    private Date created;
    private Date lastUpdate;

    public CreationDetails() {}

    public CreationDetails(String name, String value, Date created) {
        this.name = name;
        this.value = value;
        this.created = this.lastUpdate = created;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Date getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(Date lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CreationDetails that = (CreationDetails) o;

        if (!id.equals(that.id)) return false;
        if (!name.equals(that.name)) return false;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
