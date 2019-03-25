/*-
 * **************************************************-
 * InGrid Elasticsearch Tools
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import de.ingrid.ibus.client.BusClient;
import de.ingrid.ibus.client.BusClientFactory;
import de.ingrid.utils.xml.XMLSerializer;

@Service
public class IBusIndexManager implements IConfigurable, IIndexManager {

    private static final Logger log = LogManager.getLogger(IBusIndexManager.class);
    
    private List<IBus> iBusses;

    @Autowired
    private ElasticConfig config;

    public IBusIndexManager() {
        
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
        return response.getString( "result" );
    }

    @Override
    public boolean createIndex(String name) {
        
        IngridCall call = prepareCall( "createIndex" );
        Map<String,String> map = new HashMap<>();
        InputStream mappingStream = getClass().getClassLoader().getResourceAsStream( "default-mapping.json" );
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

    @Override
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

    @Override
    public void checkAndCreateInformationIndex() {
        IngridCall call = prepareCall( "checkAndCreateInformationIndex" );
        
        sendCallToIBusses(call);
    }

    @Override
    public String getIndexTypeIdentifier(IndexInfo indexInfo) {
        String componentIdentifier = indexInfo.getComponentIdentifier();
        if (componentIdentifier == null) componentIdentifier = config.communicationProxyUrl;
        String clientId = componentIdentifier.replace( "/", "" );
        return clientId + "=>" + indexInfo.getToAlias() + ":" + indexInfo.getToType();
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

    @Override
    public void deleteIndex(String index) {
        IngridCall call = prepareCall( "deleteIndex" );
        call.setParameter( index );
        
        sendCallToIBusses(call);
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
		log.error("Operation 'delete' not implemented yet!");
	}

    @Override
    public boolean indexExists(String indexName) {
        IngridCall call = prepareCall( "indexExists" );
        call.setParameter(indexName);

        IngridDocument response = sendCallToIBusses(call);
        return (boolean) response.get( "result" );
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
}
