package de.ingrid.elasticsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import de.ingrid.ibus.client.BusClient;
import de.ingrid.ibus.client.BusClientFactory;
import de.ingrid.utils.ElasticDocument;
import de.ingrid.utils.IBus;
import de.ingrid.utils.IConfigurable;
import de.ingrid.utils.IngridCall;
import de.ingrid.utils.IngridDocument;
import de.ingrid.utils.PlugDescription;

@Service
public class IBusIndexManager implements IConfigurable, IIndexManager {

    private static final Logger log = Logger.getLogger(IBusIndexManager.class);
    
    private IBus _ibus;
    
    public IBusIndexManager() {
        
    }
            
    @Override
    public void configure(PlugDescription plugDescription) {
        BusClient busClient = BusClientFactory.getBusClient();
        _ibus = busClient.getNonCacheableIBus();
    }
    
    private IBus getIBus() {
        if (_ibus == null) {
            BusClient busClient = BusClientFactory.getBusClient();
            _ibus = busClient.getNonCacheableIBus();
        }
        return _ibus;
    }

    @Override
    public String getIndexNameFromAliasName(String indexAlias, String partialName) {
        IngridCall call = new IngridCall();
        call.setMethod( "getIndexNameFromAliasName" );
        call.setTarget( "__centralIndex__" );
        Map<String,String> map = new HashMap<String, String>();
        map.put( "indexAlias", indexAlias );
        map.put( "partialName", partialName );
        call.setParameter( map );
        
        try {
            IngridDocument response = getIBus().call( call );
            return response.getString( "result" );
        } catch (Exception e) {
            log.error( "Error relaying index message: getIndexNameFromAliasName", e );
        }
        return null;
    }
    
    @Override
    public boolean createIndex(String name) {
        
        IngridCall call = prepareCall( "createIndex" );
        call.setParameter( name );
        
        try {
            IngridDocument response = getIBus().call( call );
            return response.getBoolean( "result" );
        } catch (Exception e) {
            log.error( "Error relaying index message: createIndex", e );
        }
        return false;
    }

    @Override
    public boolean createIndex(String name, String type, String source) {
        IngridCall call = prepareCall( "createIndex" );
        Map<String,String> map = new HashMap<String, String>();
        map.put( "name", name );
        map.put( "type", type );
        map.put( "source", source );
        call.setParameter( name );
        
        try {
            IngridDocument response = getIBus().call( call );
            return response.getBoolean( "result" );
        } catch (Exception e) {
            log.error( "Error relaying index message: createIndex", e );
        }
        return false;
    }

    @Override
    public void switchAlias(String aliasName, String oldIndex, String newIndex) {
        IngridCall call = prepareCall( "switchAlias" );
        Map<String,String> map = new HashMap<String, String>();
        map.put( "aliasName", aliasName );
        map.put( "oldIndex", oldIndex );
        map.put( "newIndex", newIndex );
        call.setParameter( map );
        
        try {
            getIBus().call( call );
        } catch (Exception e) {
            log.error( "Error relaying index message: switchAlias", e );
        }
    }

    @Override
    public void checkAndCreateInformationIndex() {
        IngridCall call = prepareCall( "checkAndCreateInformationIndex" );
        
        try {
            getIBus().call( call );
        } catch (Exception e) {
            log.error( "Error relaying index message: checkAndCreateInformationIndex", e );
        }
    }

    @Override
    public String getIndexTypeIdentifier(IndexInfo indexInfo) {
        IngridCall call = prepareCall( "getIndexTypeIdentifier" );
        call.setParameter( indexInfo );
        
        try {
            IngridDocument response = getIBus().call( call );
            return response.getString( "result" );
        } catch (Exception e) {
            log.error( "Error relaying index message: getIndexTypeIdentifier", e );
        }
        return null;
    }

    @Override
    public void addBasicFields(ElasticDocument document, IndexInfo info) {
        IngridCall call = prepareCall( "addBasicFields" );
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "document", document );
        map.put( "info", info );
        call.setParameter( map );
        
        try {
            getIBus().call( call );
        } catch (Exception e) {
            log.error( "Error relaying index message: addBasicFields", e );
        }
    }

    @Override
    public void update(IndexInfo indexinfo, ElasticDocument doc, boolean updateOldIndex) {
        IngridCall call = prepareCall( "update" );
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "indexinfo", indexinfo );
        map.put( "doc", doc );
        map.put( "updateOldIndex", updateOldIndex );
        call.setParameter( map );
        
        try {
            getIBus().call( call );
        } catch (Exception e) {
            log.error( "Error relaying index message: update", e );
        }
    }

    @Override
    public void updateIPlugInformation(String id, String info) throws InterruptedException, ExecutionException {
        log.warn( "The method is not implemented yet: updateIPlugInformation" );
        IngridCall call = prepareCall( "updateIPlugInformation" );
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "id", id );
        map.put( "info", info );
        call.setParameter( map );
        
        try {
            getIBus().call( call );
        } catch (Exception e) {
            log.error( "Error relaying index message: updateIPlugInformation", e );
        }
    }

    @Override
    public void flush() {
        IngridCall call = prepareCall( "flush" );
        
        try {
            getIBus().call( call );
        } catch (Exception e) {
            log.error( "Error relaying index message: flush", e );
        }
    }

    @Override
    public void deleteIndex(String index) {
        IngridCall call = prepareCall( "deleteIndex" );
        call.setParameter( index );
        
        try {
            getIBus().call( call );
        } catch (Exception e) {
            log.error( "Error relaying index message: deleteIndex", e );
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> getMapping(IndexInfo indexInfo) {
        IngridCall call = prepareCall( "getMapping" );
        call.setParameter( indexInfo );
        
        try {
            IngridDocument response = getIBus().call( call );
            return (Map<String, Object>) response.get( "result" );
        } catch (Exception e) {
            log.error( "Error relaying index message: getMapping", e );
        }
        return null;
    }

    @Override
    public void updateHearbeatInformation(List<String> iPlugIds) throws InterruptedException, ExecutionException, IOException {
        IngridCall call = prepareCall( "getIndexNameFromAliasName" );
        call.setParameter(iPlugIds);
        
        try {
            getIBus().call( call );
        } catch (Exception e) {
            log.error( "Error relaying index message: updateHearbeatInformation", e );
        }
    }
    
    /**
     * Simplify creation of IngridCall object.
     * @param method
     * @param paramsAsMap
     * @return
     */
    private IngridCall prepareCall(String method) {
        IngridCall call = new IngridCall();
        call.setTarget( "__centralIndex__" );
        call.setMethod( method );
        return call;
    }

}
