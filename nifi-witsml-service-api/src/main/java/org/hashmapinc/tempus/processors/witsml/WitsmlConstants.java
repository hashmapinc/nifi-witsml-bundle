package org.hashmapinc.tempus.processors.witsml;

import sun.java2d.pipe.SpanShapeRenderer;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.TimeZone;

public class WitsmlConstants {
    public static final String TIMEZONE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS[XXX]";

    public static SimpleDateFormat getSimpleDateTimeFormat(String timeZone){
        SimpleDateFormat timeFormat = new SimpleDateFormat();
        timeFormat.applyPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        timeFormat.setTimeZone(TimeZone.getTimeZone(ZoneId.of(timeZone)));
        return timeFormat;
    }
}
