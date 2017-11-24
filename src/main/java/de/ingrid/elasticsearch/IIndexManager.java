package de.ingrid.elasticsearch;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.xcontent.XContentBuilder;

import de.ingrid.utils.ElasticDocument;

public interface IIndexManager {

    public String getIndexNameFromAliasName(String indexAlias, String partialName);
    
    public boolean createIndex(String name);
    
    public boolean createIndex(String name, String type, String source);
    
    public void switchAlias(String aliasName, String oldIndex, String newIndex);
    
    public void checkAndCreateInformationIndex();
    
    public String getIndexTypeIdentifier(IndexInfo indexInfo);
    
    public void update(IndexInfo indexinfo, ElasticDocument doc, boolean updateOldIndex);
    
    public void updateIPlugInformation(String id, String info) throws InterruptedException, ExecutionException;
    
    public void flush();
    
    public void deleteIndex(String index);
    
    public Map<String, Object> getMapping(IndexInfo indexInfo);
    
    public void updateHearbeatInformation(Map<String, String> iPlugIdInfos) throws InterruptedException, ExecutionException, IOException;
}
