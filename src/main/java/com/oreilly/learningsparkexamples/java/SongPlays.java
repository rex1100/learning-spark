package com.oreilly.learningsparkexamples.java;

import java.io.Serializable;

/**
 * Created by brett on 04/12/16.
 */
public class SongPlays implements Serializable {
    String songId;
    int playCount;

    public SongPlays() {
    }

    public SongPlays(String songId, int playCount) {
        this.songId = songId;
        this.playCount = playCount;
    }

    public String getSongId() {
        return songId;
    }

    public void setSongId(String songId) {
        this.songId = songId;
    }

    public int getPlayCount() {
        return playCount;
    }

    public void setPlayCount(int playCount) {
        this.playCount = playCount;
    }
}
