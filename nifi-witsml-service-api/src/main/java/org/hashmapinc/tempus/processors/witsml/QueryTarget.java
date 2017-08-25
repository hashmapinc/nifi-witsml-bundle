package org.hashmapinc.tempus.processors.witsml;

import java.util.ArrayList;
import java.util.List;

public class QueryTarget {

    private QueryLevel queryLevel;
    private WitsmlObjectId well;
    private WitsmlObjectId wellbore;
    private List<String> objectsToQuery;

    public QueryLevel getQueryLevel(){
        return queryLevel;
    }

    public void setQueryLevel(QueryLevel queryLevel){
        this.queryLevel = queryLevel;
    }

    public WitsmlObjectId getWell(){
        return well;
    }

    public void setWell(WitsmlObjectId well){
        this.well = well;
    }

    public WitsmlObjectId getWellbore(){
        return wellbore;
    }

    public void setWellbore(WitsmlObjectId wellbore){
        this.wellbore = wellbore;
    }

    public List<String> getObjectsToQuery(){
        return objectsToQuery;
    }

    public void setObjectsToQuery(List<String> objectsToQuery){
        this.objectsToQuery = objectsToQuery;
    }

    public static QueryTarget parseURI(String uri, List<String> objectsToQuery){
        String[] nodes = uri.split("/");
        QueryTarget target = new QueryTarget();
        target.objectsToQuery = objectsToQuery;
        if (nodes.length == 0) {
             target.queryLevel = QueryLevel.Server;
             return target;
        }

        WitsmlObjectId well = ParseNameAndId(nodes[1], "well");
        target.well = well;
        target.queryLevel = QueryLevel.Well;

        if (nodes.length > 2) {
            WitsmlObjectId wellbore = ParseNameAndId(nodes[2], "wellbore");
            target.queryLevel = QueryLevel.Wellbore;
            target.wellbore = wellbore;
        }
        return target;
    }


    private static WitsmlObjectId ParseNameAndId(String nodePath, String type){
        String[] node = nodePath.split("\\(");
        String name = node[0];
        String id = node[1].replace(")", "");
        return new WitsmlObjectId(name, id, type, "");
    }
}
