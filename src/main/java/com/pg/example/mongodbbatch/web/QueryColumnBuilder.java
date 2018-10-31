package com.pg.example.mongodbbatch.web;

import com.pg.example.mongodbbatch.util.StringUtils;

/**Created by Pawel Gawedzki on 9/21/2017.*/
public class QueryColumnBuilder {
    private String select;
    private String from;
    private String limit;

    private QueryColumnBuilder select(String select) {
        this.select = select;
        return this;
    }

    private QueryColumnBuilder from(String from) {
        this.from = from;
        return this;
    }

    private QueryColumnBuilder limit(int limit) {
        this.limit = String.valueOf(limit);
        return this;
    }

    public static QueryColumnBuilder queryForEquipmentCargoColumns(String branch) {
        return new QueryColumnBuilder()
                .select("*")
                .from("`" + branch + "`.`/data_sets/equipment_cargo`")
                .limit(1);
    }

    public static QueryColumnBuilder queryForOperationalRoutesColumns(String branch) {
        return new QueryColumnBuilder()
                .select("*")
                .from("`" + branch + "`.`/data_sets/operational_routes`")
                .limit(1);
    }

    public static QueryColumnBuilder queryForHaulageInfoColumns(String branch) {
        return new QueryColumnBuilder()
                .select("*")
                .from("`" + branch + "`.`/data_sets/haulage_info`")
                .limit(1);
    }

    public static QueryColumnBuilder queryForHaulageEquipmentColumns(String branch) {
        return new QueryColumnBuilder()
                .select("*")
                .from("`" + branch + "`.`/datasources/haulage_equipment`")
                .limit(1);
    }

    public static QueryColumnBuilder queryForGlobalBookingsTruckinglegsColumns(String branch) {
        return new QueryColumnBuilder()
                .select("*")
                .from("`" + branch + "`.`/data_sets/global_bookings_truckinglegs`")
                .limit(1);
    }

    public String build() {
        if (!StringUtils.nullOrEmpty(limit)) {
            limit = "1";
        }
        String selectQuery = "SELECT " + select + " FROM " + from + " LIMIT " + limit;
        return String.format("{\"query\" : \"%s\"}", selectQuery);
    }

    @Override
    public String toString() {
        return this.build();
    }
}
