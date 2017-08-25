package org.hashmapinc.tempus.processors.witsml;

import java.util.ArrayList;
import java.util.List;

public class WitsmlObjectId {

    private String name;
    private String id;
    private String type;
    private String uri;

    public WitsmlObjectId(String name, String id, String type) {
        this.name = name;
        this.id = id;
        this.type = type;
        this.uri =  "/" + getName() + "(" + getId() + ")";

    }

    public String getName(){
        return name;
    }

    public String getId(){
        return id;
    }

    public String getType(){
        return type;
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
