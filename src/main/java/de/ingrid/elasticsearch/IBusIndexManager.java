/*-
 * **************************************************-
 * InGrid Elasticsearch Tools
 * ==================================================
 * Copyright (C) 2014 - 2022 wemove digital solutions GmbH
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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import de.ingrid.utils.*;
import de.ingrid.utils.query.IngridQuery;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import de.ingrid.ibus.client.BusClient;
import de.ingrid.ibus.client.BusClientFactory;
import de.ingrid.utils.xml.XMLSerializer;

@Service
public class IBusIndexManager implements IConfigurable, IIndexManager {

    private static final Logger log = LogManager.getLogger(IBusIndexManager.class);
    
    private List<IBus> iBusses;

    private final ElasticConfig config;

    public IBusIndexManager(ElasticConfig config) {
        this.config = config;
    }
            
    @Override
    public void configure(PlugDescription plugDescription) {
        BusClient busClient = BusClientFactory.getBusClient();
        iBusses = busClient.getNonCacheableIBusses();
    }
    
    private List<IBus> getIBusses() {
        if (iBusses == null) {
            BusClient busClient = BusClientFactory.getBusClient();
            iBusses = busClient.getNonCacheableIBusses();
        }
        return iBusses;
    }

    @Override
    public String getIndexNameFromAliasName(String indexAlias, String partialName) {
        IngridCall call = new IngridCall();
        call.setMethod( "getIndexNameFromAliasName" );
        call.setTarget( "__centralIndex__" );
        Map<String,String> map = new HashMap<>();
        map.put( "indexAlias", indexAlias );
        map.put( "partialName", partialName );
        call.setParameter( map );
        
        IngridDocument response = sendCallToIBusses(call);
        return response != null ? response.getString("result") : null;
    }
    
    public String getIndexNameFromAliasName(int iBusIndex, String indexAlias, String partialName) {
        IngridCall call = new IngridCall();
        call.setMethod( "getIndexNameFromAliasName" );
        call.setTarget( "__centralIndex__" );
        Map<String,String> map = new HashMap<>();
        map.put( "indexAlias", indexAlias );
        map.put( "partialName", partialName );
        call.setParameter( map );
        
        IngridDocument response = sendCallToIBus(iBusses.get(iBusIndex), call);
        return response != null ? response.getString("result") : null;
    }

    @Override
    public boolean createIndex(String name) {
        
        IngridCall call = prepareCall( "createIndex" );
        Map<String,String> map = new HashMap<>();
        InputStream mappingStream = getClass().getClassLoader().getResourceAsStream( "default-mapping.json" );

        if (mappingStream == null) {
            throw new RuntimeException("default-mapping.json file was not found when creating index");
        }

        map.put( "name", name );
        try {
            map.put( "mapping", XMLSerializer.getContents( mappingStream ) );
        } catch (IOException e1) {
            log.error( "Error converting stream to string", e1 );
            return false;
        }
        
        call.setParameter( map );
        
        IngridDocument response = sendCallToIBusses(call);
        return response.getBoolean( "result" );
    }

    public boolean createIndex(int iBusIndex, String name, String type, String esMapping, String esSettings) {
        IngridCall call = prepareCall( "createIndex" );
        Map<String,String> map = new HashMap<>();
        map.put( "name", name );
        map.put( "type", type );
        map.put( "esMapping", esMapping );
        map.put( "esSettings", esSettings );
        call.setParameter( map );
        
        IngridDocument response = sendCallToIBus(iBusses.get(iBusIndex), call);
        return response != null && response.getBoolean("result");
    }
    
    public boolean createIndex(String name, String type, String esMapping, String esSettings) {
        IngridCall call = prepareCall( "createIndex" );
        Map<String,String> map = new HashMap<>();
        map.put( "name", name );
        map.put( "type", type );
        map.put( "esMapping", esMapping );
        map.put( "esSettings", esSettings );
        call.setParameter( map );
        
        IngridDocument response = sendCallToIBusses(call);
        return response.getBoolean( "result" );
    }

    @Override
    public void switchAlias(String aliasName, String oldIndex, String newIndex) {
        IngridCall call = prepareCall( "switchAlias" );
        Map<String,String> map = new HashMap<>();
        map.put( "aliasName", aliasName );
        map.put( "oldIndex", oldIndex );
        map.put( "newIndex", newIndex );
        call.setParameter( map );
        
        sendCallToIBusses(call);
    }
    
    public void switchAlias(int iBusIndex, String aliasName, String oldIndex, String newIndex) {
        IngridCall call = prepareCall( "switchAlias" );
        Map<String,String> map = new HashMap<>();
        map.put( "aliasName", aliasName );
        map.put( "oldIndex", oldIndex );
        map.put( "newIndex", newIndex );
        call.setParameter( map );

        sendCallToIBus(iBusses.get(iBusIndex), call);
    }

    @Override
    public void checkAndCreateInformationIndex() {
        IngridCall call = prepareCall( "checkAndCreateInformationIndex" );
        
        sendCallToIBusses(call);
    }

    @Override
    public String getIndexTypeIdentifier(IndexInfo indexInfo) {
        return config.uuid + "=>" + indexInfo.getToIndex() + ":" + indexInfo.getToType();
    }

    @Override
    public void update(IndexInfo indexinfo, ElasticDocument doc, boolean updateOldIndex) {
        IngridCall call = prepareCall( "update" );
        Map<String, Object> map = new HashMap<>();
        map.put( "indexinfo", indexinfo );
        map.put( "doc", doc );
        map.put( "updateOldIndex", updateOldIndex );
        call.setParameter( map );
        
        sendCallToIBusses(call);
    }
    
    public void update(int iBusIndex, IndexInfo indexinfo, ElasticDocument doc, boolean updateOldIndex) {
        IngridCall call = prepareCall( "update" );
        Map<String, Object> map = new HashMap<>();
        map.put( "indexinfo", indexinfo );
        map.put( "doc", doc );
        map.put( "updateOldIndex", updateOldIndex );
        call.setParameter( map );

        sendCallToIBus(iBusses.get(iBusIndex), call);
    }

    @Override
    public void updatePlugDescription(PlugDescription plugDescription) throws IOException {
        log.warn("Not implemented");
    }

    @Override
    public void updateIPlugInformation(String id, String info) throws InterruptedException, ExecutionException {
        IngridCall call = prepareCall( "updateIPlugInformation" );
        Map<String, Object> map = new HashMap<>();
        map.put( "id", id );
        map.put( "info", info );
        call.setParameter( map );
        
        sendCallToIBusses(call);
    }

    @Override
    public void flush() {
        IngridCall call = prepareCall( "flush" );
        
        sendCallToIBusses(call);
    }
    
    
    public void flush(int iBusIndex) {
        IngridCall call = prepareCall( "flush" );
        sendCallToIBus(iBusses.get(iBusIndex), call);
    }

    @Override
    public void deleteIndex(String index) {
        IngridCall call = prepareCall( "deleteIndex" );
        call.setParameter( index );
        
        sendCallToIBusses(call);
    }
    
    public void deleteIndex(int iBusIndex, String index) {
        IngridCall call = prepareCall( "deleteIndex" );
        call.setParameter( index );

        sendCallToIBus(iBusses.get(iBusIndex), call);
    }

    @Override
    public String[] getIndices(String filter) {
        IngridCall call = prepareCall( "getIndices" );
        call.setParameter( filter );

        IngridDocument response = sendCallToIBusses(call);
        return (String[]) response.get( "result" );
    }
    
    public String[] getIndices(int iBusIndex, String filter) {
        IngridCall call = prepareCall( "getIndices" );
        call.setParameter( filter );

        IngridDocument response = sendCallToIBus(iBusses.get(iBusIndex), call);
        return (String[]) (response != null ? response.get("result") : null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> getMapping(IndexInfo indexInfo) {
        IngridCall call = prepareCall( "getMapping" );
        call.setParameter( indexInfo );
        
        IngridDocument response = sendCallToIBusses(call);
        return (Map<String, Object>) response.get( "result" );
    }

    @Override
    public String getDefaultMapping() {
        InputStream mappingStream = getClass().getClassLoader().getResourceAsStream( "default-mapping.json" );
        try {
            if (mappingStream != null) {
                return XMLSerializer.getContents( mappingStream );
            }
        } catch (IOException e) {
            log.error( "Error getting default mapping for index creation", e );
        }
        return null;
    }

    @Override
    public String getDefaultSettings() {
        InputStream settingsStream = getClass().getClassLoader().getResourceAsStream( "default-settings.json" );
        try {
            if (settingsStream != null) {
                return XMLSerializer.getContents( settingsStream );
            }
        } catch (IOException e) {
            log.error( "Error getting default mapping for index creation", e );
        }
        return null;
    }

    @Override
    public void updateHearbeatInformation(Map<String, String> iPlugIdInfos) throws InterruptedException, ExecutionException, IOException {
        IngridCall call = prepareCall( "updateHearbeatInformation" );
        call.setParameter(iPlugIdInfos);
        
        sendCallToIBusses(call);
    }

	@Override
	public void delete(IndexInfo indexinfo, String id, boolean updateOldIndex) {
        IngridCall call = prepareCall( "deleteDocById" );
        Map<String, Object> map = new HashMap<>();
        map.put( "indexinfo", indexinfo );
        map.put( "id", id );
        map.put( "updateOldIndex", updateOldIndex );
        call.setParameter( map );

        sendCallToIBusses(call);
	}
    
	public void delete(int iBusIndex, IndexInfo indexinfo, String id, boolean updateOldIndex) {
        IngridCall call = prepareCall( "deleteDocById" );
        Map<String, Object> map = new HashMap<>();
        map.put( "indexinfo", indexinfo );
        map.put( "id", id );
        map.put( "updateOldIndex", updateOldIndex );
        call.setParameter( map );

        sendCallToIBus(iBusses.get(iBusIndex), call);
	}

    @Override
    public boolean indexExists(String indexName) {
        IngridCall call = prepareCall( "indexExists" );
        call.setParameter(indexName);

        IngridDocument response = sendCallToIBusses(call);
        return (boolean) response.get( "result" );
    }
    
    public boolean indexExists(int iBusIndex, String indexName) {
        IngridCall call = prepareCall( "indexExists" );
        call.setParameter(indexName);

        IngridDocument response = sendCallToIBus(iBusses.get(iBusIndex), call);
        return (boolean) (response != null ? response.get("result") : false);
    }

    public IngridHits search(IngridQuery query, int start, int length) {
        IngridCall call = prepareCall( "search" );

        Map<String, Object> map = new HashMap<>();
        map.put( "query", query );
        map.put( "start", start );
        map.put( "length", length );
        call.setParameter( map );

        IngridDocument response = sendCallToIBusses(call);
        return (IngridHits) response.get( "result" );
    }

    public IngridHitDetail getDetail(IngridHit hit, IngridQuery query, String[] fields) {
        IngridCall call = prepareCall( "getDetail" );

        Map<String, Object> map = new HashMap<>();
        map.put( "hit", hit );
        map.put( "query", query );
        map.put( "fields", fields );
        call.setParameter( map );

        IngridDocument response = sendCallToIBusses(call);
        return (IngridHitDetail) response.get( "result" );
    }

    public IngridHitDetail[] getDetails(IngridHit[] hits, IngridQuery query, String[] fields) {
        IngridCall call = prepareCall( "getDetails" );

        Map<String, Object> map = new HashMap<>();
        map.put( "hits", hits );
        map.put( "query", query );
        map.put( "fields", fields );
        call.setParameter( map );

        IngridDocument response = sendCallToIBusses(call);
        return (IngridHitDetail[]) response.get( "result" );
    }

    private IngridDocument sendCallToIBusses(IngridCall call) {

        IngridDocument response = null;
        for ( IBus ibus : getIBusses()) {
            try {
                IngridDocument currentResponse = ibus.call( call );
                if (response == null) {
                    response = currentResponse;
                }
            } catch (Exception e) {
                log.error( "Error relaying index message: " + call.getMethod(), e );
            }
        }
        return response;

    }

    private IngridDocument sendCallToIBus(IBus iBus, IngridCall call) {

        try {
            return iBus.call( call );
        } catch (Exception e) {
            log.error( "Error relaying index message: " + call.getMethod(), e );
            return null;
        }

    }

    /**
     * Simplify creation of IngridCall object.
     * @param method
     * @return
     */
    private IngridCall prepareCall(String method) {

        IngridCall call = new IngridCall();
        call.setTarget( "__centralIndex__" );
        call.setMethod( method );
        return call;

    }

    public ElasticDocument getDocById(String documentId) {
        IngridCall call = prepareCall( "getDocById" );
        call.setParameter(documentId);

        IngridDocument response = sendCallToIBusses(call);
        return (ElasticDocument) response.get( "result" );
    }
}
