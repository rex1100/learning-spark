package com.oreilly.learningsparkexamples.java;

/**
 * Created by brett on 08/12/16.
 */
public class SongYear {

    String trackId;
    int year;
    int decade;

    public SongYear(String trackId, int year) {
        this.trackId = trackId;
        this.year = year;
        decade = (year/10) * 10; // this should remove the last digit to give a decade
    }

    public int getDecade() {
        return decade;
    }

    public void setDecade(int decade) {
        this.decade = decade;
    }

    public String getTrackId() {
        return trackId;
    }

    public void setTrackId(String trackId) {
        this.trackId = trackId;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }
}
