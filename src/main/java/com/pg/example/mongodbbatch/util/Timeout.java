package com.pg.example.mongodbbatch.util;

/* Created by Pawel Gawedzki on 22-Mar-18.*/
public enum Timeout {
    SECONDS("s", 1000),
    MINUTES("m", 60 * 1000),
    HOURS("h", 60 * 60 * 1000),
    DEFAULT("", HOURS.multiplier);

    private String units;
    private int multiplier;
    Timeout(String units, int multiplier) {
        this.units = units;
        this.multiplier = multiplier;
    }

    public static int timeout(String value) {
        Timeout timeout = valueFor(value);
        int intValue = Integer.parseInt(value.substring(0, value.length() - 1));
        return intValue * timeout.multiplier;
    }

    private static Timeout valueFor(String value) {
        if (value == null || value.trim().isEmpty()) {
            return DEFAULT;
        }
        for (Timeout timeout : Timeout.values()) {
            if (value.toLowerCase().endsWith(timeout.units)) {
                return timeout;
            }
        }
        return DEFAULT;
    }
}
