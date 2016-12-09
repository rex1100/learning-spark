package com.oreilly.learningsparkexamples.java;

/**
 * Created by brett on 08/12/16.
 */
public class SongYear {

    String trackId;
    int year;

    public SongYear(String trackId, int year) {
        this.trackId = trackId;
        this.year = year;
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
