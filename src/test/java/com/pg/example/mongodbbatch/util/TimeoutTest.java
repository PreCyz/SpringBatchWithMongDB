package com.pg.example.mongodbbatch.util;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;


/* Created by Pawel Gawedzki on 22-Mar-18.*/
public class TimeoutTest {

    @Test
    public void givenMinutesTimeout_when_timeout_then_returnValue() {
        String m = "2m";

        assertThat(Timeout.timeout(m), is(equalTo(2 * 60 * 1000)));
    }

    @Test
    public void givenSecondsTimeout_when_timeout_then_returnValue() {
        String m = "45s";

        assertThat(Timeout.timeout(m), is(equalTo(45 * 1000)));
    }

    @Test
    public void givenHoursTimeout_when_timeout_then_returnValue() {
        String m = "45H";

        assertThat(Timeout.timeout(m), is(equalTo(45 * 60 * 60 * 1000)));
    }
}