package org.hashmapinc.tempus.processors.witsml;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class WitsmlObjectId {

    private String name;
    private String id;
    private String type;
    private String uri;
    private String objectData;
    private LocalDateTime lastModified;

    public WitsmlObjectId(String name, String id, String type, String parentUri, String objectData) {
        this.name = name;
        this.id = id;
        this.type = type;
        this.uri =  parentUri + "/" + getName() + "(" + getId() + ")";
        this.lastModified = lastModified;
        this.objectData = objectData;
    }

    public WitsmlObjectId(String name, String id, String type, String parentUri, LocalDateTime lastModified, String objectData) {
        this(name, id, type, parentUri, objectData);
        this.lastModified = lastModified;
    }

    public String getName(){
        return name;
    }

    public String getId(){
        return id;
    }

    public String getData() {return objectData;}

    public String getType(){
        return type;
    }

    public LocalDateTime getLastModified() {
        return lastModified;
    }

    public String getUri(){ return uri; }

    @Override
    public boolean equals(Object obj) {
        if (!obj.getClass().equals(this.getClass()))
            return false;

        WitsmlObjectId source = (WitsmlObjectId)obj;
        return source.name.equals(this.name);
    }
}
