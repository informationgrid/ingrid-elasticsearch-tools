/*
 * **************************************************-
 * ingrid-base-webapp
 * ==================================================
 * Copyright (C) 2014 - 2018 wemove digital solutions GmbH
 * ==================================================
 * Licensed under the EUPL, Version 1.1 or – as soon they will be
 * approved by the European Commission - subsequent versions of the
 * EUPL (the "Licence");
 * 
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * http://ec.europa.eu/idabc/eupl5
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 * **************************************************#
 */
package de.ingrid.elasticsearch;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import de.ingrid.utils.ElasticDocument;
import de.ingrid.utils.xml.XMLSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Utility class to manage elasticsearch indices and documents.
 * @author Andre
 *
 */
@Service
public class IndexManager implements IIndexManager {
    private static final Logger log = LogManager.getLogger( IndexManager.class );

    private ElasticConfig _config;

    private Client _client;
    
    private BulkProcessor _bulkProcessor;
    
    private Map<String, String> iPlugDocIdMap;
    
    @Autowired
    public IndexManager(ElasticsearchNodeFactoryBean elastic, ElasticConfig config) {

        // do not initialize when using central index
        if (config.esCommunicationThroughIBus) return;
        
        _config = config;
        _client = elastic.getClient();
        iPlugDocIdMap = new HashMap<>();
    }

    @PostConstruct
    public void postConstruct() {
        // do not initialize when using central index
        if (_config.esCommunicationThroughIBus) return;

        _bulkProcessor = BulkProcessor
                .builder( _client, getBulkProcessorListener() )
                .setFlushInterval( TimeValue.timeValueSeconds(5L) )
                .build();
    }

    /**
     * Insert or update a document to the lucene index. For updating the documents must be indexed by its ID and the global configuration
     * "indexWithAutoId" (index.autoGenerateId) has to be disabled.
     * 
     * @param indexinfo
     *            contains information about the index to be used besided other information
     * @param doc
     *            is the document to be indexed
     * @param updateOldIndex
     *            if true, it'll be checked if the current index differs from the real index, which is used during reindexing
     */
    public void update(IndexInfo indexinfo, ElasticDocument doc, boolean updateOldIndex) {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index( indexinfo.getRealIndexName() ).type( indexinfo.getToType() );

        if (!_config.indexWithAutoId) {
            indexRequest.id( (String) doc.get( indexinfo.getDocIdField() ) );
        }

        _bulkProcessor.add( indexRequest.source( doc ) );

        if (updateOldIndex) {
            String oldIndex = getIndexNameFromAliasName( indexinfo.getToIndex(), null );
            // if the current index differs from the real index, then it means there's an indexing going on
            // and if the real index name is the same as the index alias, it means that no complete indexing happened yet
            if ((oldIndex != null) && !oldIndex.equals( indexinfo.getRealIndexName() ) && (!indexinfo.getToIndex().equals( indexinfo.getRealIndexName() ))) {
                IndexInfo otherIndexInfo = indexinfo.clone();
                otherIndexInfo.setRealIndexName( oldIndex );
                update( otherIndexInfo, doc, false );
            }
        }
    }

    /**
     * Delete a document with a given ID from an index/type. The ID must not be autogenerated but a unique ID from the source document.
     * 
     * @param indexinfo describes the index to be used
     * @param id is the ID of the document to be deleted
     * @param updateOldIndex if true then also remove document from previous index in case we're indexing right now
     */
    public void delete(IndexInfo indexinfo, String id, boolean updateOldIndex) {
        DeleteRequest deleteRequest = new DeleteRequest();
        deleteRequest.index( indexinfo.getRealIndexName() ).type( indexinfo.getToType() ).id( id );

        _bulkProcessor.add( deleteRequest );

        if (updateOldIndex) {
            String oldIndex = getIndexNameFromAliasName( indexinfo.getToIndex(), null );
            if (!oldIndex.equals( indexinfo.getRealIndexName() )) {
                IndexInfo otherIndexInfo = indexinfo.clone();
                otherIndexInfo.setRealIndexName( oldIndex );
                delete( otherIndexInfo, id, false );
            }
        }
    }

    private Listener getBulkProcessorListener() {
        return new BulkProcessor.Listener() {

            public void beforeBulk(long executionId, BulkRequest request) {}
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {}

            public void afterBulk(long executionId, BulkRequest request, Throwable t) {
                log.error( "An error occured during bulk indexing", t );
            }

        };
    }

    public void flush() {
        _bulkProcessor.flush();
    }

    /**
     * This function does not seem to be used anywhere
     */
    @Deprecated
    public void flushAndClose() {
        _bulkProcessor.flush();
        _bulkProcessor.close();
    }

    public void switchAlias(String aliasName, String oldIndex, String newIndex) {
        // check if alias actually exists
        // boolean aliasExists = _client.admin().indices().aliasesExist( new GetAliasesRequest( aliasName ) ).actionGet().exists();
        if (oldIndex != null)
            removeFromAlias( aliasName, oldIndex );
        IndicesAliasesRequestBuilder prepareAliases = _client.admin().indices().prepareAliases();
        prepareAliases.addAlias( newIndex, aliasName ).execute().actionGet();
    }

    public void addToAlias(String aliasName, String newIndex) {
        IndicesAliasesRequestBuilder prepareAliases = _client.admin().indices().prepareAliases();
        prepareAliases.addAlias( newIndex, aliasName ).execute().actionGet();
    }

    public void removeFromAlias(String aliasName, String index) {
        String indexNameFromAliasName = getIndexNameFromAliasName( aliasName, index );
        while (indexNameFromAliasName != null) {
            IndicesAliasesRequestBuilder prepareAliases = _client.admin().indices().prepareAliases();
            prepareAliases.removeAlias( indexNameFromAliasName, aliasName ).execute().actionGet();
            indexNameFromAliasName = getIndexNameFromAliasName( aliasName, index );
        }
    }

    public void removeAlias(String aliasName) {
        removeFromAlias( aliasName, null );
    }

    /**
     * Function does not seem to be used!?
     */
    @Deprecated
    public boolean typeExists(String indexName, String type) {
        TypesExistsRequest typeRequest = new TypesExistsRequest( new String[] { indexName }, type );
        try {
            return _client.admin().indices().typesExists( typeRequest ).actionGet().isExists();
        } catch (IndexNotFoundException e) {
            return false;
        }
    }

    public void deleteIndex(String index) {
        _client.admin().indices().prepareDelete( index ).execute().actionGet();
    }

    // type will not be used soon anymore
    // use createIndex(String name, String source) instead
    @Deprecated
    public boolean createIndex(String name, String type, String source) {
        boolean indexExists = indexExists( name );
        if (!indexExists) {
            
            if (source != null) {
                _client.admin().indices().prepareCreate( name )
                    .addMapping( type, source, XContentType.JSON )
                    .execute().actionGet();
            } else {
                _client.admin().indices().prepareCreate( name )
                .execute().actionGet();
            }
            return true;
            
        }
        return false;
    }

    public boolean createIndex(String name, String source) {
        boolean indexExists = indexExists( name );
        if (!indexExists) {

            if (source != null) {
                _client.admin().indices().prepareCreate( name )
                    .addMapping( "_default_", source, XContentType.JSON )
                    .execute().actionGet();
            } else {
                _client.admin().indices().prepareCreate( name )
                .execute().actionGet();
            }
            return true;

        }
        return false;
    }
    
    public boolean createIndex(String name) {
        InputStream defaultMappingStream = getClass().getClassLoader().getResourceAsStream( "default-mapping.json" );
        if (defaultMappingStream == null) {
            log.warn("Could not find mapping file 'default-mapping.json' for creating index: " + name);
            return createIndex( name, null );

        } else {
            try {
                String source = XMLSerializer.getContents( defaultMappingStream );
                return createIndex( name, source );

            } catch (IOException e) {
                log.error( "Error getting default mapping for index creation", e );
            }
        }

        return false;
    }

    public boolean indexExists(String name) {
        return _client.admin().indices().prepareExists( name ).execute().actionGet().isExists();
    }

    /**
     * Get the index matching the partial name from an alias.
     * 
     * @param indexAlias
     *            is the alias name to check for connected indices
     * @param partialName
     *            is the first part of the index to be matched, if there are several
     * @return the found index name
     */
    public String getIndexNameFromAliasName(String indexAlias, String partialName) {

        ImmutableOpenMap<String, List<AliasMetaData>> indexToAliasesMap = _client.admin().indices().getAliases( new GetAliasesRequest( indexAlias ) ).actionGet().getAliases();

        if (indexToAliasesMap != null && !indexToAliasesMap.isEmpty()) {
            Iterator<ObjectCursor<String>> iterator = indexToAliasesMap.keys().iterator();
            String result = null;
            while (iterator.hasNext()) {
                String next = iterator.next().value;
                if (partialName == null || next.contains(partialName)) {
                    result = next;
                }
            }

            return result;
        } else if (_client.admin().indices().prepareExists( indexAlias ).execute().actionGet().isExists()) {
            // alias seems to be the index itself
            return indexAlias;
        }
        return null;
    }

    public Map<String, Object> getMapping(IndexInfo indexInfo) {
        String indexName = getIndexNameFromAliasName( indexInfo.getRealIndexName(), null );
        ClusterState cs = _client.admin().cluster().prepareState().setIndices( indexName ).execute().actionGet().getState();
        MappingMetaData mappingMetaData = cs.getMetaData().index( indexName ).mapping( indexInfo.getToType() );
        if (mappingMetaData == null) {
            return null;
        } else {
            return mappingMetaData.getSourceAsMap();
        }
    }

    public void refreshIndex(String indexName) {
        _client.admin().indices().refresh( new RefreshRequest( indexName ) ).actionGet();
    }

    public Client getClient() {
        return _client;
    }

    public String printSettings() throws Exception {
        return _client.settings().toDelimitedString( ',' );
    }

    public void shutdown() throws Exception {
        _client.close();
    }

    @Override
    public void checkAndCreateInformationIndex() {
        if (!indexExists( "ingrid_meta" )) {
            InputStream ingridMetaMappingStream = getClass().getClassLoader().getResourceAsStream( "ingrid-meta-mapping.json" );
            if (ingridMetaMappingStream == null) {
                log.error("Could not find mapping file 'ingrid-meta-mapping.json' for creating index 'ingrid_meta'");
            } else {
                try {
                    String source = XMLSerializer.getContents(ingridMetaMappingStream);
                    createIndex("ingrid_meta", "info", source);
                } catch (IOException e) {
                    log.error("Could not deserialize: ingrid-meta-mapping.json", e);
                }
            }
        }
    }

    @Override
    public String getIndexTypeIdentifier(IndexInfo indexInfo) {
        String componentIdentifier = indexInfo.getComponentIdentifier();
        if (componentIdentifier == null) componentIdentifier = _config.communicationProxyUrl;
        String clientId = componentIdentifier.replace( "/", "" );
        return clientId + "=>" + indexInfo.getToAlias() + ":" + indexInfo.getToType();
    }

    @Override
    public void updateIPlugInformation(String id, String info) throws InterruptedException, ExecutionException {
        String docId = iPlugDocIdMap.get( id );
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index( "ingrid_meta" ).type( "info" );

        if (docId == null) {
            SearchResponse response = _client.prepareSearch( "ingrid_meta" )
                    .setTypes( "info" )
                    .setQuery( QueryBuilders.termQuery( "plugId", id ) )
                    // .setFetchSource( new String[] { "*" }, null )
                    .setSize( 1 )
                    .get();

            long totalHits = response.getHits().getTotalHits();

            // do update document
            if (totalHits == 1) {
                docId = response.getHits().getAt( 0 ).getId();
                iPlugDocIdMap.put( id, docId );
                UpdateRequest updateRequest = new UpdateRequest( "ingrid_meta", "info", docId );
                // indexRequest.id( docId );
                // add index request to queue to avoid sending of too many requests
                _bulkProcessor.add( updateRequest.doc( info, XContentType.JSON ) );
            } else if (totalHits == 0) {
                // create document immediately so that it's available for further requests
                docId = _client.index( indexRequest.source( info, XContentType.JSON ) ).get().getId();
                iPlugDocIdMap.put( id, docId );
            } else {
                log.error( "There is more than one iPlug information document in the index of: " + id );
            }

        } else {
            // indexRequest.id( docId );
            UpdateRequest updateRequest = new UpdateRequest( "ingrid_meta", "info", docId );
            _bulkProcessor.add( updateRequest.doc( info, XContentType.JSON ) );
        }
    }

    @Override
    public void updateHearbeatInformation(Map<String, String> iPlugIdInfos) throws InterruptedException, ExecutionException {
    	checkAndCreateInformationIndex();
        for (String id : iPlugIdInfos.keySet()) {
            try {
                updateIPlugInformation(id, iPlugIdInfos.get(id));
            } catch (IndexNotFoundException ex) {
                log.warn( "Index for iPlug information not found ... creating: " + id );
            }
        }
    }
    
    /**
     * Generate a new ID for a new index of the format <index-name>_<id>, where <id> is number counting up.
     * 
     * @param name is the name of the index without the timestamp
     * @return a new index name including the current timestamp
     */
    public static String getNextIndexName(String name) {
        if (name == null) {
            throw new RuntimeException( "Old index name must not be null!" );
        }
        boolean isNew = false;

        SimpleDateFormat dateFormat = new SimpleDateFormat( "yyyyMMddHHmmssS" );

        int delimiterPos = name.lastIndexOf( "_" );
        if (delimiterPos == -1) {
            isNew = true;
        } else {
            try {
                dateFormat.parse( name.substring( delimiterPos + 1 ) );
            } catch (Exception ex) {
                isNew = true;
            }
        }

        String date = dateFormat.format( new Date() );

        if (isNew) {
            return name + "_" + date;
        } else {
            return name.substring( 0, delimiterPos + 1 ) + date;
        }
    }
    
}
