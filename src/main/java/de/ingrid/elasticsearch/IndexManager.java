/*
 * **************************************************-
 * ingrid-base-webapp
 * ==================================================
 * Copyright (C) 2014 - 2024 wemove digital solutions GmbH
 * ==================================================
 * Licensed under the EUPL, Version 1.2 or – as soon they will be
 * approved by the European Commission - subsequent versions of the
 * EUPL (the "Licence");
 *
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 * https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 * **************************************************#
 */
package de.ingrid.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch.cat.indices.IndicesRecord;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.get_alias.IndexAliases;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.elasticsearch.indices.update_aliases.Action;
import co.elastic.clients.elasticsearch.indices.update_aliases.AddAction;
import co.elastic.clients.elasticsearch.indices.update_aliases.RemoveAction;
import co.elastic.clients.json.JsonData;
import de.ingrid.utils.ElasticDocument;
import de.ingrid.utils.IngridDocument;
import de.ingrid.utils.PlugDescription;
import de.ingrid.utils.xml.XMLSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Utility class to manage elasticsearch indices and documents.
 *
 * @author Andre
 */
@Service
public class IndexManager implements IIndexManager {
    private static final Logger log = LogManager.getLogger(IndexManager.class);

    private ElasticConfig _config;

    private ElasticsearchClient _client;

    private BulkIngester<String> _bulkProcessor;

    @Autowired
    private ElasticsearchNodeFactoryBean esBean;


    @Autowired
    public IndexManager(ElasticConfig config) {
        _config = config;
    }

    @PostConstruct
    public void init() {
        // do not initialize when using central index
        if (_config.esCommunicationThroughIBus) return;

        _client = esBean.getClient();
        _bulkProcessor = BulkIngester.of(bi -> bi
                .listener(getBulkProcessorListener())
                .flushInterval(5l, TimeUnit.SECONDS)
        );
    }

    /**
     * Insert or update a document to the lucene index. For updating the documents must be indexed by its ID and the global configuration
     * "indexWithAutoId" (index.autoGenerateId) has to be disabled.
     *
     * @param indexinfo      contains information about the index to be used besided other information
     * @param doc            is the document to be indexed
     * @param updateOldIndex if true, it'll be checked if the current index differs from the real index, which is used during reindexing
     */
    public void update(IndexInfo indexinfo, ElasticDocument doc, boolean updateOldIndex) {
        IndexOperation.Builder<ElasticDocument> updateOperation = new IndexOperation.Builder<ElasticDocument>()
                .index(indexinfo.getRealIndexName())
                .document(doc);

        if (!_config.indexWithAutoId) {
            updateOperation.id((String) doc.get(indexinfo.getDocIdField()));
        }

        _bulkProcessor.add(BulkOperation.of(b -> b.index(updateOperation.build())));

        if (updateOldIndex) {
            String oldIndex = getIndexNameFromAliasName(indexinfo.getToAlias(), null);
            // if the current index differs from the real index, then it means there's an indexing going on
            // and if the real index name is the same as the index alias, it means that no complete indexing happened yet
            if ((oldIndex != null) && !oldIndex.equals(indexinfo.getRealIndexName()) && (!indexinfo.getToIndex().equals(indexinfo.getRealIndexName()))) {
                IndexInfo otherIndexInfo = indexinfo.clone();
                otherIndexInfo.setRealIndexName(oldIndex);
                update(otherIndexInfo, doc, false);
            }
        }
    }

    /**
     * Delete a document with a given ID from an index/type. The ID must not be autogenerated but a unique ID from the source document.
     *
     * @param indexinfo      describes the index to be used
     * @param id             is the ID of the document to be deleted
     * @param updateOldIndex if true then also remove document from previous index in case we're indexing right now
     */
    public void delete(IndexInfo indexinfo, String id, boolean updateOldIndex) {

        _bulkProcessor.add(BulkOperation.of(bulk -> bulk
                .delete(DeleteOperation.of(x -> x.index(indexinfo.getRealIndexName()).id(id))))
        );

        if (updateOldIndex) {
            String oldIndex = getIndexNameFromAliasName(indexinfo.getToAlias(), null);
            if (oldIndex != null && !oldIndex.equals(indexinfo.getRealIndexName())) {
                IndexInfo otherIndexInfo = indexinfo.clone();
                otherIndexInfo.setRealIndexName(oldIndex);
                delete(otherIndexInfo, id, false);
            }
        }
    }

    private BulkListener<String> getBulkProcessorListener() {
        return new BulkListener<>() {

            @Override
            public void beforeBulk(long executionId, BulkRequest request, List<String> contexts) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, List<String> contexts, BulkResponse response) {
                // The request was accepted, but may contain failed items.
                // The "context" list gives the file name for each bulk item.
                for (int i = 0; i < contexts.size(); i++) {
                    BulkResponseItem item = response.items().get(i);
                    if (item.error() != null) {
                        // Inspect the failure cause
                        log.error("Failed to index file " + contexts.get(i) + " - " + item.error().reason());
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, List<String> contexts, Throwable failure) {
                // The request could not be sent
                log.error("Bulk request " + executionId + " failed", failure);
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
        if (oldIndex != null)
            removeFromAlias(aliasName, oldIndex);
        addToAlias(aliasName, newIndex);
    }

    public void addToAlias(String aliasName, String newIndex) {
        try {
            _client.indices().updateAliases(ua -> ua.actions(Action.of(a -> a
                    .add(AddAction.of(add -> add
                            .alias(aliasName)
                            .index(newIndex))))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void removeFromAlias(String aliasName, String index) {
        String indexNameFromAliasName = getIndexNameFromAliasName(aliasName, index);
        while (indexNameFromAliasName != null) {

            try {
                _client.indices().updateAliases(ua -> ua.actions(Action.of(a -> a
                        .remove(RemoveAction.of(rem -> rem
                                .alias(aliasName)
                                .index(index))))));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            indexNameFromAliasName = getIndexNameFromAliasName(aliasName, index);
        }
    }

    public void removeAlias(String aliasName) {
        removeFromAlias(aliasName, null);
    }

    public void deleteIndex(String index) {
        try {
            _client.delete(DeleteRequest.of(d -> d.index(index)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String[] getIndices(String filter) {
        List<IndicesRecord> indicesRecords = null;
        try {
            indicesRecords = this._client.cat().indices().valueBody();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return indicesRecords.stream()
                .map(IndicesRecord::index)
                .filter(index -> index.contains(filter))
                .toArray(String[]::new);
    }

    // type will not be used soon anymore
    // use createIndex(String name, String esMapping) instead?
    // @Deprecated
    public boolean createIndex(String name, String esMapping, String esSettings) {
        boolean indexExists = indexExists(name);
        if (!indexExists) {

            CreateIndexRequest.Builder request = new CreateIndexRequest.Builder().index(name);

            if (esMapping != null) {
                request.mappings(m -> m.withJson(new StringReader(esMapping)));

                if (esSettings != null) {
                    request.settings(s -> s.withJson(new StringReader(esSettings)));
                }
            }

            try {
                _client.indices().create(request.build());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return true;
        }
        return false;
    }

    public boolean createIndex(String name, String source) {
        boolean indexExists = indexExists(name);
        if (!indexExists) {

            CreateIndexRequest.Builder request = new CreateIndexRequest.Builder().index(name);

            if (source != null) {
                request.mappings(m -> m.withJson(new StringReader(source)));
            }
            try {
                _client.indices().create(request.build());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return true;

        }
        return false;
    }

    public boolean createIndex(String name) {
        InputStream defaultMappingStream = getClass().getClassLoader().getResourceAsStream("default-mapping.json");
        if (defaultMappingStream == null) {
            log.warn("Could not find mapping file 'default-mapping.json' for creating index: " + name);
            return createIndex(name, null);

        } else {
            try {
                String source = XMLSerializer.getContents(defaultMappingStream);
                return createIndex(name, source);

            } catch (IOException e) {
                log.error("Error getting default mapping for index creation", e);
            }
        }

        return false;
    }

    public String getDefaultMapping() {

        InputStream defaultMappingStream = getClass().getClassLoader().getResourceAsStream("default-mapping.json");
        if (defaultMappingStream != null) {
            try {
                return XMLSerializer.getContents(defaultMappingStream);
            } catch (IOException e) {
                log.error("Error deserializing default mapping file", e);
            }
        }
        return null;

    }

    public String getDefaultSettings() {

        InputStream defaultSettingsStream = getClass().getClassLoader().getResourceAsStream("default-settings.json");
        if (defaultSettingsStream != null) {
            try {
                return XMLSerializer.getContents(defaultSettingsStream);
            } catch (IOException e) {
                log.error("Error deserializing default settings file", e);
            }
        }
        return null;

    }

    public boolean indexExists(String name) {
        try {
            return _client.indices().exists(ex -> ex.index(name)).value();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the index matching the partial name from an alias.
     *
     * @param indexAlias  is the alias name to check for connected indices
     * @param partialName is the first part of the index to be matched, if there are several
     * @return the found index name
     */
    public String getIndexNameFromAliasName(String indexAlias, String partialName) {

        Map<String, IndexAliases> indexToAliasesMap;
        try {
            indexToAliasesMap = _client.indices().getAlias(ar -> ar.name(indexAlias)).result();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (indexToAliasesMap != null && !indexToAliasesMap.isEmpty()) {
            Iterator<String> iterator = indexToAliasesMap.keySet().iterator();
            String result = null;
            while (iterator.hasNext()) {
                String next = iterator.next();
                if (partialName == null || next.contains(partialName)) {
                    result = next;
                }
            }

            return result;
        } else if (indexExists(indexAlias)) {
            // alias seems to be the index itself
            return indexAlias;
        }
        return null;
    }

    public Map<String, Object> getMapping(IndexInfo indexInfo) {
        String indexName = getIndexNameFromAliasName(indexInfo.getRealIndexName(), null);
        IndexMappingRecord mappingMetaData;
        try {
            mappingMetaData = _client.indices().getMapping(m -> m.index(indexName)).get(indexName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (mappingMetaData == null) {
            return null;
        } else {
            return null; // TODO AW: mappingMetaData.mappings().properties();
        }
    }

    public void refreshIndex(String indexName) {
        try {
            _client.indices().refresh(r -> r.index(indexName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ElasticsearchClient getClient() {
        return _client;
    }

    public String printSettings() {
        try {
            return _client.indices().getSettings().toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        _client.shutdown();
    }

    /**
     * <p>Check if index ingrid_meta exists. If not create it.</p>
     *
     * <p>Applies mappings from <strong>required</strong> ingrid-meta-mapping.json found in classpath.</p>
     *
     * <p>Applies settings from optional ingrid-meta-settings.json found in classpath.</p>
     */
    @Override
    public void checkAndCreateInformationIndex() {
        if (!indexExists("ingrid_meta")) {
            try (InputStream ingridMetaMappingStream = getClass().getClassLoader().getResourceAsStream("ingrid-meta-mapping.json")) {
                if (ingridMetaMappingStream == null) {
                    log.error("Could not find mapping file 'ingrid-meta-mapping.json' for creating index 'ingrid_meta'");
                } else {
                    // settings are optional
                    String settings = null;
                    try (InputStream ingridMetaSettingsStream = getClass().getClassLoader().getResourceAsStream("ingrid-meta-settings.json")) {
                        if (ingridMetaSettingsStream != null) {
                            settings = XMLSerializer.getContents(ingridMetaSettingsStream);
                        }
                    } catch (IOException e) {
                        log.warn("Could not deserialize: ingrid-meta-settings.json, continue without settings.", e);
                    }

                    String mapping = XMLSerializer.getContents(ingridMetaMappingStream);
                    createIndex("ingrid_meta", mapping, settings);
                }
            } catch (IOException e) {
                log.error("Could not deserialize: ingrid-meta-mapping.json", e);
            }
        }
    }

    @Override
    public String getIndexTypeIdentifier(IndexInfo indexInfo) {
        return _config.uuid + "=>" + indexInfo.getToIndex();
    }


    public IngridDocument getAllIPlugInformation() {
        SearchResponse<ElasticDocument> response = null;
        try {
            response = _client.search(s -> s
                            .index("ingrid_meta")
                            .size(1000)
                            .source(fs -> fs.fetch(true))
                    , ElasticDocument.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        HitsMetadata<ElasticDocument> hits = response.hits();
        List<IngridDocument> iPlugInfos = new ArrayList<>();

        for (int i = 0; i < hits.total().value(); i++) {
            IngridDocument doc = mapIPlugInformatioToIngridDocument(hits.hits().get(i));
            iPlugInfos.add(doc);
        }

        IngridDocument result = new IngridDocument();
        result.put("iPlugInfos", iPlugInfos);
        return result;

    }

    public IngridDocument getIPlugInformation(String plugId) {
        SearchResponse<ElasticDocument> response = null;
        try {
            response = _client.search(s -> s
                            .index("ingrid_meta")
                            .query(TermQuery.of(tq -> tq.field("plugId").value(plugId))._toQuery())
                            // .setFetchSource( new String[] { "*" }, null )
                            .size(1)
                            .source(fs -> fs.fetch(true))
                    //.storedFields("iPlugName", "datatype", "fields")
                    , ElasticDocument.class
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        HitsMetadata<ElasticDocument> hits = response.hits();

        if (hits.total().value() > 0) {
            return mapIPlugInformatioToIngridDocument(hits.hits().get(0));
        }

        return null;
    }

    private IngridDocument mapIPlugInformatioToIngridDocument(Hit<ElasticDocument> hit) {
        IngridDocument doc = new IngridDocument();

        Map<String, JsonData> sourceAsMap = hit.fields();
        doc.put("plugId", sourceAsMap.get("plugId"));
        doc.put("name", sourceAsMap.get("iPlugName"));
        doc.put("plugdescription", sourceAsMap.get("plugdescription"));
        return doc;
    }

    @Override
    public void updatePlugDescription(PlugDescription plugDescription) throws IOException {
        String uuid = (String) plugDescription.get("uuid");

        SearchResponse<ElasticDocument> response = _client.search(s -> s
                .index("ingrid_meta")
                .query(QueryBuilders.wildcard(wq -> wq.field("indexId").value(uuid))), ElasticDocument.class
        );

        plugDescription.remove("METADATAS");
        JsonData jsonData = JsonData.fromJson("{ \"plugdescription\": " + plugDescription + "}");

        HitsMetadata<ElasticDocument> hits = response.hits();
        if (hits.total().value() > 2) {
            log.warn("There are more than 2 documents found for indexId starting with " + uuid);
        }
        hits.hits().forEach(hit -> {
            _bulkProcessor.add(BulkOperation.of(b -> b.update(ur -> ur
                    .index("ingrid_meta")
                    .id(hit.id())
                    .action(a -> a.upsert(jsonData))))
            );
        });

    }

    @Override
    public void updateIPlugInformation(String id, String info) throws InterruptedException, ExecutionException {
        synchronized (this) {
            String docId;

            // the iPlugDocIdMap can lead to problems if a wrong ID was stored once, then the iBus has to be restarted
            SearchResponse<ElasticDocument> response = null;
            try {
                response = _client.search(s -> s
                                .index("ingrid_meta")
                                .query(TermQuery.of(tq -> tq.field("indexId").value(id))._toQuery())
                                .sort(SortOptions.of(so -> so.field(f -> f
                                        .field("lastIndexed")
                                        .order(SortOrder.Desc))))// sort to get most current on top
                        // .setFetchSource( new String[] { "*" }, null )
                        // .setSize(1)
                        , ElasticDocument.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            HitsMetadata<ElasticDocument> hits = response.hits();
            long totalHits = hits.total().value();

            // do update document
            if (totalHits == 1) {
                docId = hits.hits().get(0).id();
                // add index request to queue to avoid sending of too many requests
                _bulkProcessor.add(BulkOperation.of(b -> b.update(ur -> ur
                        .index("ingrid_meta")
                        .id(docId)
                        .action(a -> a.upsert(info)))));
            } else if (totalHits == 0) {
                // create document immediately so that it's available for further requests
                try {
                    _client.index(IndexRequest.of(ir -> ir.index("ingrid_meta").document(info)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                log.warn("There is more than one iPlug information document in the index of: " + id);
                log.warn("Removing items and adding new one");
                List<Hit<ElasticDocument>> searchHits = hits.hits();
                // delete all hits except the first one
                for (int i = 1; i < searchHits.size(); i++) {
                    Hit<ElasticDocument> hit = searchHits.get(i);
                    _bulkProcessor.add(BulkOperation.of(b -> b
                            .delete(d -> d
                                    .index("ingrid_meta")
                                    .id(hit.id())))
                    );
                }
                flush();

                // add first hit, which we did not delete
                _bulkProcessor.add(BulkOperation.of(b -> b
                        .update(u -> u
                                .index("ingrid_meta")
                                .id(searchHits.get(0).id())
                                .action(a -> a.upsert(info))))
                );
            }
        }
    }

    @Override
    public void updateHearbeatInformation(Map<String, String> iPlugIdInfos) throws ExecutionException {
        checkAndCreateInformationIndex();
        for (String id : iPlugIdInfos.keySet()) {
            try {
                updateIPlugInformation(id, iPlugIdInfos.get(id));
//            } catch (IndexNotFoundException ex) {
//                log.warn( "Index for iPlug information not found ... creating: " + id );
            } catch (InterruptedException ex) {
                log.warn("updateHearbeatInformation was interrupted for ID: " + id);
            }
        }
    }

    /**
     * Generate a new ID for a new index of the format <index-name>_<id>, where <id> is number counting up.
     *
     * @param name is the name of the index without the timestamp
     * @param uuid is a uuid to make this index unique (uuid or name from iPlug)
     * @return a new index name including the current timestamp
     */
    public static String getNextIndexName(String name, String uuid, String uuidName) {
        if (name == null) {
            throw new RuntimeException("Old index name must not be null!");
        }
        uuidName = uuidName.toLowerCase();
        boolean isNew = false;

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssS");

        int delimiterPos = name.lastIndexOf("_");
        if (delimiterPos == -1) {
            isNew = true;
        } else {
            try {
                dateFormat.parse(name.substring(delimiterPos + 1));
            } catch (Exception ex) {
                isNew = true;
            }
        }

        String date = dateFormat.format(new Date());

        if (isNew) {
            if (!name.contains(uuid)) {
                return name + "@" + uuidName + "-" + uuid + "_" + date;
            }
            return name + "_" + date;
        } else {
            if (!name.contains(uuid)) {
                return name.substring(0, delimiterPos) + "@" + uuidName + "-" + uuid + "_" + date;
            }
            return name.substring(0, delimiterPos + 1) + date;
        }
    }

    public ElasticDocument getDocById(Object id) {
        String idAsString = String.valueOf(id);
        IndexInfo[] indexNames = _config.activeIndices;
        // iterate over all indices until document was found
        for (IndexInfo indexName : indexNames) {
            try {
                Map<String, Object> source = _client.get(g -> g.index(indexName.getRealIndexName()).id(idAsString)
                                .source(s -> s.fields(List.of(_config.indexFieldsIncluded.split(","))))
                        , ElasticDocument.class).source();

                if (source != null) {
                    return new ElasticDocument(source);
                }
            } catch (Exception ex) {
                log.warn("Index was not found. We probably have to clean up or refresh the active indices here. Missing index is: " + indexName.getToAlias());
            }
        }

        return null;
    }

}
