package com.pg.example.mongodbbatch.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TimeLog {

    private final static Logger logger = LoggerFactory.getLogger(TimeLog.class);

    private static long nextTimeLogId = 0;

    private long id;
    private String key;
    private long startTime;
    private long lastTime;

    private String currentOperation;

    private List<String> times;

    public TimeLog(String key) {
        times = new ArrayList<>();

        this.id = nextTimeLogId++;
        this.key = key;
        this.startTime = System.currentTimeMillis();
        this.lastTime = this.startTime;

        currentOperation = "init";
    }

    private String getLabelString(String label) {
        int spacesToAdd = 24 - label.length();
        if (spacesToAdd <= 0) {
            return label;
        } else {
            StringBuilder result = new StringBuilder(label);
            for (int i = 0; i < spacesToAdd; i++) {
                result.append(" ");
            }
            return result.toString();
        }
    }

    private String getTimeStr(String label) {
        long currentTime = System.currentTimeMillis();
        double elapsedTime = (double) (currentTime - startTime) / (1000);
        double elapsedTimeSinceLast = (double) (currentTime - lastTime) / (1000);
        lastTime = currentTime;
        return String.format("%s [%d]: %s: %.3fs (%.3fs)", key, id, getLabelString(label), elapsedTimeSinceLast, elapsedTime);
    }

    public void logTime(String label) {
        times.add(getTimeStr(currentOperation));
        currentOperation = label;
    }

    public synchronized void done() {
        logTime(null);

        for (String time : times) {
            logger.trace(time);
        }
    }
}
