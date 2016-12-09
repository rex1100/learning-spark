package com.oreilly.learningsparkexamples.java;

import java.util.List;

/**
 * Created by brett on 06/12/16.
 */
public class ArtistAndList {
    String artistId;
    List<String> valueList;

    public ArtistAndList() {
    }

    public ArtistAndList(String ArtistId, List<String> valueList) {

        this.artistId = ArtistId;
        this.valueList = valueList;
    }

    public String getArtistId() {

        return artistId;
    }

    public void setArtistId(String artistId) {
        this.artistId = artistId;
    }

    public List<String> getValueList() {
        return valueList;
    }

    public void setValueList(List<String> similarArtistIds) {
        this.valueList = similarArtistIds;
    }
}
