package com.oreilly.learningsparkexamples.java;

import java.util.List;

/**
 * Created by brett on 06/12/16.
 */
public class ArtistAndStringList {
    String artistId;
    String valueList;

    public ArtistAndStringList() {
    }

    public ArtistAndStringList(String ArtistId, String valueList) {

        this.artistId = ArtistId;
        this.valueList = valueList;
    }

    public String getArtistId() {

        return artistId;
    }

    public void setArtistId(String artistId) {
        this.artistId = artistId;
    }

    public String getValueList() {
        return valueList;
    }

    public void setValueList(String similarArtistIds) {
        this.valueList = similarArtistIds;
    }
}
