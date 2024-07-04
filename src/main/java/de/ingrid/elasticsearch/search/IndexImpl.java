/*
 * **************************************************-
 * ingrid-iplug-se-iplug
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
package de.ingrid.elasticsearch.search;

import co.elastic.clients.elasticsearch._types.ShardFailure;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.mapping.FieldType;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.FunctionScoreQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.*;
import co.elastic.clients.json.JsonData;
import de.ingrid.elasticsearch.ElasticConfig;
import de.ingrid.elasticsearch.IndexInfo;
import de.ingrid.elasticsearch.IndexManager;
import de.ingrid.elasticsearch.QueryBuilderService;
import de.ingrid.elasticsearch.search.converter.QueryConverter;
import de.ingrid.utils.*;
import de.ingrid.utils.dsc.Column;
import de.ingrid.utils.dsc.Record;
import de.ingrid.utils.query.IngridQuery;
import jakarta.json.JsonValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.stream.Stream;

@Component
public class IndexImpl implements ISearcher, IDetailer, IRecordLoader {

    private static final Logger log = LogManager.getLogger(IndexImpl.class);

    private final QueryBuilderService queryBuilderService;

    private final ElasticConfig config;

    private final QueryConverter queryConverter;

    private final FacetConverter facetConverter;

    private static final String ELASTIC_SEARCH_INDEX = "es_index";

    private static final String ELASTIC_SEARCH_INDEX_TYPE = "es_type";

    private final String[] detailFields;

    private final IndexManager indexManager;


    @Autowired
    public IndexImpl(ElasticConfig config, IndexManager indexManager, QueryConverter qc, FacetConverter fc, QueryBuilderService queryBuilderService) {
        this.indexManager = indexManager;
        this.config = config;
        this.queryBuilderService = queryBuilderService;

        detailFields = Stream.concat(
                        Arrays.stream(new String[]{
                                PlugDescription.PARTNER,
                                PlugDescription.PROVIDER,
                                "datatype",
                                PlugDescription.DATA_SOURCE_NAME}),
                        Arrays.stream(config.additionalSearchDetailFields))
                .toArray(String[]::new);

        this.queryConverter = qc;
        this.facetConverter = fc;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public IngridHits search(IngridQuery ingridQuery, int startHit, int num) {

        // convert InGrid-query to QueryBuilder
        BoolQuery.Builder query = queryConverter.convert(ingridQuery);

        FunctionScoreQuery.Builder funcScoreQuery;
        if (config.indexEnableBoost) {
            funcScoreQuery = queryConverter.addScoreModifier(query.build()._toQuery());
        } else {
            funcScoreQuery = null;
        }

        boolean isLocationSearch = containsBoundingBox(ingridQuery);
        boolean hasFacets = ingridQuery.containsKey("FACETS");

        // request grouping information from index if necessary
        // see IndexImpl.getHitsFromResponse for usage
        String groupedBy = ingridQuery.getGrouped();
        String[] fields = null;
        if (IngridQuery.GROUPED_BY_PARTNER.equalsIgnoreCase(groupedBy)) {
            fields = new String[]{IngridQuery.PARTNER};
        } else if (IngridQuery.GROUPED_BY_ORGANISATION.equalsIgnoreCase(groupedBy)) {
            fields = new String[]{IngridQuery.PROVIDER};
        }/* else if (IngridQuery.GROUPED_BY_DATASOURCE.equalsIgnoreCase( groupedBy )) {
            // the necessary value id the results ID
        }*/

        IndexInfo[] indexInfos = this.config.activeIndices;

        if (indexInfos.length == 0) {
            log.debug("No configured index to search on!");
            return new IngridHits(0, new IngridHit[0]);
        }

        // if we are remotely connected to an elasticsearch node then get the real indices of the aliases
        // otherwise we also get the results from other indices, since an alias can contain several indices!
        List<String> realIndices = new ArrayList<>();
        for (IndexInfo indexInfo : indexInfos) {
            String realIndex = indexManager.getIndexNameFromAliasName(
                    indexInfo.getToAlias(),
                    indexInfo.getRealIndexName() == null ? indexInfo.getToAlias() : indexInfo.getRealIndexName());

            if (realIndex != null && !realIndices.contains(realIndex)) {
                realIndices.add(realIndex);
            }
        }
        String[] realIndexNames = realIndices.toArray(new String[0]);

        BoolQuery.Builder indexTypeFilter = queryBuilderService.createIndexTypeFilter(indexInfos);

        // Filter for results only with location information
        if (isLocationSearch) {
            BoolQuery.Builder boolShould = QueryBuilders.bool();
            boolShould.must(QueryBuilders.exists(e -> e.field("x1")));
            indexTypeFilter.filter(boolShould.build()._toQuery());
        }


        // search prepare
        SearchRequest.Builder srb = new SearchRequest.Builder()
                .index(Arrays.asList(realIndexNames))
                // .setQuery( config.indexEnableBoost ? funcScoreQuery : query ) // Query
                .query(config.indexEnableBoost
                        ? QueryBuilders.bool().must(funcScoreQuery.build()._toQuery()).must(indexTypeFilter.build()._toQuery()).build()._toQuery()
                        : QueryBuilders.bool().must(query.build()._toQuery()).must(indexTypeFilter.build()._toQuery()).build()._toQuery()) // Query
                .storedFields("iPlugId")
                .from(startHit).size(num).explain(false);

        // Add sort by date to ES query if appropriate
        String rankingType = ingridQuery.getRankingType();
        if (rankingType != null && rankingType.equals(IngridQuery.DATE_RANKED)) {
            srb.sort(List.of(
                    SortOptions.of(so -> so
                            .field(f -> f
                                    .field("modified")
                                    .order(SortOrder.Desc)
                                    .unmappedType(FieldType.Date)
                            )),
                    SortOptions.of(so -> so
                            .field(f -> f
                                    .field("sort_hash")
                                    .order(SortOrder.Asc)
                                    .missing("_last")
                                    .unmappedType(FieldType.Keyword)
                            ))
            ));
        } else {
            srb.sort(List.of(
                    SortOptions.of(so -> so
                            .score(s -> s.order(SortOrder.Desc))
                    ),
                    SortOptions.of(so -> so
                            .field(f -> f
                                    .field("sort_hash")
                                    .order(SortOrder.Asc)
                                    .missing("_last")
                                    .unmappedType(FieldType.Keyword)
                            ))
            ));
        }

        if (fields == null) {
            srb = srb.source(s -> s.fetch(false));
        } else {
            srb = srb.storedFields(List.of(fields));
        }

        // pre-processing: add facets/aggregations to the query
        if (hasFacets) {
            srb.aggregations(facetConverter.getAggregations(ingridQuery));
        }

        if (config.trackTotalHits) {
            srb.trackTotalHits(t -> t.enabled(true));
        }

        SearchRequest searchRequest = srb.build();
        if (log.isDebugEnabled()) {
            log.debug("Final Elastic Search Query: \n" + searchRequest);
        }

        // search!
        try {
            SearchResponse<ElasticDocument> searchResponse = indexManager.getClient().search(searchRequest, ElasticDocument.class);

            // convert to IngridHits
            IngridHits hits = getHitsFromResponse(searchResponse, ingridQuery);

            // post-processing: extract and convert facets to InGrid-Document
            if (hasFacets) {
                // add facets from response
                IngridDocument facets = facetConverter.convertFacetResultsToDoc(searchResponse);
                hits.put("FACETS", facets);
            }

            return hits;
        } catch (IOException ex) {
            log.error("Search failed on indices: " + Arrays.toString(realIndexNames), ex);
            return new IngridHits(0, new IngridHit[0]);
        }
    }

    private boolean containsBoundingBox(IngridQuery ingridQuery) {
        boolean found = ingridQuery.containsField("x1");

        // also try to look in clauses
        if (!found) {
            for (IngridQuery clause : ingridQuery.getAllClauses()) {
                if (clause.containsField("x1")) {
                    return true;
                }
            }
        }
        return found;
    }

    /**
     * Create InGrid hits from ES hits. Add grouping information.
     */
    private IngridHits getHitsFromResponse(SearchResponse<ElasticDocument> searchResponse, IngridQuery ingridQuery) {
        for (ShardFailure failure : searchResponse.shards().failures()) {
            log.error("Error searching in index: " + failure.reason());
        }

        HitsMetadata<ElasticDocument> hits = searchResponse.hits();

        // the size will not be bigger than it was requested in the query with
        // 'num'
        // so we can convert from long to int here!
        int length = hits.hits().size();
        long totalHits = hits.total().value();
        IngridHit[] hitArray = new IngridHit[length];
        int pos = 0;

        if (log.isDebugEnabled()) {
            log.debug("Received " + length + " from " + totalHits + " hits.");
        }

        String groupBy = ingridQuery.getGrouped();
        for (Hit<ElasticDocument> hit : hits.hits()) {
            IngridHit ingridHit = new IngridHit(hit.fields().get("iPlugId").to(List.class).get(0).toString(), hit.id(), -1, hit.score().floatValue());
            ingridHit.put(ELASTIC_SEARCH_INDEX, hit.index());
//            ingridHit.put( ELASTIC_SEARCH_INDEX_TYPE, hit.getType() );

            // get grouing information, add if exist
            String groupValue = null;
            if (IngridQuery.GROUPED_BY_PARTNER.equalsIgnoreCase(groupBy)) {
                JsonData field = hit.fields().get(IngridQuery.PARTNER);
                if (field != null) {
                    groupValue = field.toString();
                }
            } else if (IngridQuery.GROUPED_BY_ORGANISATION.equalsIgnoreCase(groupBy)) {
                JsonData field = hit.fields().get(IngridQuery.PROVIDER);
                if (field != null) {
                    groupValue = field.toString();
                }
            } else if (IngridQuery.GROUPED_BY_DATASOURCE.equalsIgnoreCase(groupBy)) {
                groupValue = config.communicationProxyUrl;
                if (config.groupByUrl) {
                    try {
                        groupValue = new URL(hit.id()).getHost();
                    } catch (MalformedURLException e) {
                        log.warn("can not group url: " + groupValue, e);
                    }
                }
            }
            if (groupValue != null) {
                ingridHit.addGroupedField(groupValue);
            }

            hitArray[pos] = ingridHit;
            pos++;
        }

        return new IngridHits((int) totalHits, hitArray);
    }

    @Override
    public IngridHitDetail getDetail(IngridHit hit, IngridQuery ingridQuery, String[] requestedFields) {
        for (int i = 0; i < requestedFields.length; i++) {
            requestedFields[i] = requestedFields[i].toLowerCase();
        }
        String documentId = hit.getDocumentId();
        String fromIndex = hit.getString(ELASTIC_SEARCH_INDEX);
        String[] allFields = Stream
                .concat(Arrays.stream(detailFields), Arrays.stream(requestedFields))
                .filter(Objects::nonNull)
                .toArray(String[]::new);

        // We have to search here again, to get a highlighted summary of the result!
        BoolQuery.Builder query = QueryBuilders.bool()
                .must(QueryBuilders.match(m -> m.field(IngridDocument.DOCUMENT_UID).query(documentId)))
                .must(queryConverter.convert(ingridQuery).build()._toQuery());

        // search prepare
        SearchRequest.Builder srb = new SearchRequest.Builder()
                .index(fromIndex)
                .source(s -> s.fetch(true))
                .query(query.build()._toQuery()) // Query
                .from(0)
                .size(1)
                .storedFields(List.of(allFields))
                .explain(false);

        if (Arrays.asList(allFields).contains(config.indexFieldSummary)) {
            srb = srb.highlight(Highlight.of(h -> h
//                    .type(HighlighterType.Unified)
                    .fields(config.indexFieldSummary+"*", HighlightField.of(hf -> hf))
            ));
        }

        SearchResponse<ElasticDocument> searchResponse = null;
        try {
            SearchRequest build = srb.build();
            searchResponse = indexManager.getClient().search(build, ElasticDocument.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        HitsMetadata<ElasticDocument> dHits = searchResponse.hits();
        return createDetail(hit, dHits.hits().get(0), allFields);
    }

    private IngridHitDetail createDetail(IngridHit hit, Hit<ElasticDocument> dHit, String[] requestedFields) {

        String title = "untitled";
        if (dHit.fields().get(config.indexFieldTitle) != null) {
            title = getStringValue(dHit.fields().get(config.indexFieldTitle));
        }
        String summary = "";
        // try to get the summary first from the highlighted fields
        if (dHit.highlight().keySet().stream().anyMatch(k -> k.startsWith(config.indexFieldSummary))) {
            List<String> stringFragments = new ArrayList<>();
            for (String fragment : dHit.highlight().entrySet().stream().filter(e -> e.getKey().startsWith(config.indexFieldSummary)).findAny().get().getValue()) {
                stringFragments.add(fragment.toString());
            }
            summary = String.join(" ... ", stringFragments);
            // otherwise get it from the original field
        } else if (dHit.fields().get(config.indexFieldSummary) != null) {
            summary = getStringValue(dHit.fields().get(config.indexFieldSummary));
        }

        IngridHitDetail detail = new IngridHitDetail(hit, title, summary);

        String dataSourceName = dHit.fields().get(PlugDescription.DATA_SOURCE_NAME).toString();

        if (dataSourceName == null) {
            log.error("The field dataSourceName could not be fetched from search index. This index field has to be stored! " +
                    "Check the index mapping file of the component.");
            throw new RuntimeException("DataSourceName not found in SearchHit. Possibly wrong mapping where index field is not stored.");
        }

        detail.setDataSourceName(dataSourceName);
        detail.setArray("datatype", getStringArrayFromSearchHit(dHit, "datatype"));
        detail.setArray(PlugDescription.PARTNER, getStringArrayFromSearchHit(dHit, PlugDescription.PARTNER));
        detail.setArray(PlugDescription.PROVIDER, getStringArrayFromSearchHit(dHit, PlugDescription.PROVIDER));

        detail.setDocumentId(hit.getDocumentId());
        for (String field : requestedFields) {
            if (detail.get(field) == null) {
                if (dHit.fields().get(field) != null) {
                    if (dHit.fields().get(field).toJson().asJsonArray() != null) {
                        if (dHit.fields().get(field).toJson().asJsonArray().size() > 1) {
                            detail.put(field, dHit.fields().get(field).to(List.class));
                        } else {
                            if (dHit.fields().get(field).toJson().getValueType() == JsonValue.ValueType.STRING) {
                                detail.put(field, new String[]{dHit.fields().get(field).to(String.class)});
                            } else {
                                detail.put(field, dHit.fields().get(field).to(List.class));
                            }
                        }
                    } else if (dHit.fields().get(field).toJson().getValueType() == JsonValue.ValueType.STRING) {
                        detail.put(field, new String[]{dHit.fields().get(field).to(String.class)});
                    } else {
                        detail.put(field, dHit.fields().get(field).to(List.class));
                    }
                }
            }
        }

        // add additional fields to detail object (such as url for iPlugSE)
        for (String extraDetail : config.additionalSearchDetailFields) {
            if (detail.containsKey(extraDetail)) continue;

            JsonData field = dHit.fields().get(extraDetail);
            if (field != null) {
                detail.put(extraDetail, field.to(List.class));
            }
        }

        return detail;
    }

    private String getStringValue(JsonData jsonData) {
        return jsonData.to(List.class).get(0).toString();
    }

    private String[] getStringArrayFromSearchHit(Hit<ElasticDocument> hit, String field) {
        JsonData jsonField = hit.fields().get(field);
        if (jsonField == null) {
            log.warn("SearchHit does not contain field: {}", field);
            return new String[0];
        }
        List<String> fieldObj = jsonField.to(List.class);
        return fieldObj.toArray(new String[0]);
    }

    @Override
    public IngridHitDetail[] getDetails(IngridHit[] hits, IngridQuery ingridQuery, String[] requestedFields) {
        // TODO: use this optimization for fewer requests, especially when requesting map markers
        /*String fromIndex = null;
        String fromType = null;
        List<IngridHitDetail> details = new ArrayList<>();
        for (int i = 0; i < requestedFields.length; i++) {
            requestedFields[i] = requestedFields[i].toLowerCase();
        }
        BoolQueryBuilder query = QueryBuilders
                .boolQuery()
                .must( queryConverter.convert( ingridQuery ) );

        for (IngridHit hit : hits) {
            String documentId = hit.getDocumentId();
            fromIndex = hit.getString( ELASTIC_SEARCH_INDEX );
            fromType = hit.getString( ELASTIC_SEARCH_INDEX_TYPE );
            query.should(QueryBuilders.matchQuery(IngridDocument.DOCUMENT_UID, documentId));
        }
        if (fromIndex != null && fromType != null) {
            // search prepare
            SearchRequestBuilder srb = indexManager.getClient().prepareSearch( fromIndex )
                    .setSearchType( SearchType.DEFAULT )
                    .setQuery( query ) // Query
                    .setFrom( 0 )
                    .setSize( hits.length )
                    .storedFields( requestedFields )
                    .setExplain( false );

            if(Arrays.stream(requestedFields).anyMatch(config.indexFieldSummary::equals)) {
                srb = srb.highlighter( new HighlightBuilder().field(config.indexFieldSummary) );
            }
            SearchResponse searchResponse = srb.execute().actionGet();

            SearchHits dHits = searchResponse.getHits();
            for (int i = 0; i < hits.length; i++) {
                IngridHit hit = hits[i];
                SearchHit dHit = dHits.getAt(i);
                details.add( createDetail(hit, dHit, requestedFields) );

            }
        }*/

        for (int i = 0; i < requestedFields.length; i++) {
            requestedFields[i] = requestedFields[i].toLowerCase();
        }
        List<IngridHitDetail> details = new ArrayList<>();
        for (IngridHit hit : hits) {
            details.add(getDetail(hit, ingridQuery, requestedFields));
        }
        return details.toArray(new IngridHitDetail[0]);
    }

    @Override
    // FIXME: is destroyed automatically via the BEAN!!!
    public void close() {
        try {
            indexManager.shutdown();
        } catch (Exception e) {
            log.error("Error shutting down IndexManager", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Record getRecord(IngridHit hit) {
        String documentId = hit.getDocumentId();
        ElasticDocument document = indexManager.getDocById(documentId);
        String[] fields = document.keySet().toArray(new String[0]);
        Record record = new Record();
        for (String name : fields) {
            Object stringValue = document.get(name);
            if (stringValue instanceof List) {
                for (String item : (List<String>) stringValue) {
                    Column column = new Column(null, name, null, true);
                    column.setTargetName(name);
                    record.addColumn(column, item);
                }
            } else {
                Column column = new Column(null, name, null, true);
                column.setTargetName(name);
                record.addColumn(column, stringValue);
            }
        }
        return record;
    }

}
