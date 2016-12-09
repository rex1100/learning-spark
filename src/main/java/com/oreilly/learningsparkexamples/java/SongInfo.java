package com.oreilly.learningsparkexamples.java;

import java.io.Serializable;

/**
 * Created by brett on 04/12/16.
 */
public class SongInfo implements Serializable {

    String trackId;
    String title;
    String songId;
    String release;
    String artistId;
    String artistMbid;
    String artistName;
    double duration;
    double artistFamiliarity;
    double artistHotttnesss;
    int year;

    public SongInfo() {
    }

    public SongInfo(String trackId, String title, String songId, String release, String artistId, String artistMbid, String artistName, double duration, double artistFamiliarity, double artistHotttnesss, int year) {
        this.trackId = trackId;
        this.title = title;
        this.songId = songId;
        this.release = release;
        this.artistId = artistId;
        this.artistMbid = artistMbid;
        this.artistName = artistName;
        this.duration = duration;
        this.artistFamiliarity = artistFamiliarity;
        this.artistHotttnesss = artistHotttnesss;
        this.year = year;
    }

    public String getTrackId() {
        return trackId;
    }

    public void setTrackId(String trackId) {
        this.trackId = trackId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getSongId() {
        return songId;
    }

    public void setSongId(String songId) {
        this.songId = songId;
    }

    public String getRelease() {
        return release;
    }

    public void setRelease(String release) {
        this.release = release;
    }

    public String getArtistId() {
        return artistId;
    }

    public void setArtistId(String artistId) {
        this.artistId = artistId;
    }

    public String getArtistMbid() {
        return artistMbid;
    }

    public void setArtistMbid(String artistMbid) {
        this.artistMbid = artistMbid;
    }

    public String getArtistName() {
        return artistName;
    }

    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }

    public double getDuration() {
        return duration;
    }

    public void setDuration(double duration) {
        this.duration = duration;
    }

    public double getArtistFamiliarity() {
        return artistFamiliarity;
    }

    public void setArtistFamiliarity(double artistFamiliarity) {
        this.artistFamiliarity = artistFamiliarity;
    }

    public double getArtistHotttnesss() {
        return artistHotttnesss;
    }

    public void setArtistHotttnesss(double artistHotttnesss) {
        this.artistHotttnesss = artistHotttnesss;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }
}
