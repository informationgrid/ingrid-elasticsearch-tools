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

import de.ingrid.elasticsearch.ElasticConfig;
import de.ingrid.elasticsearch.IndexInfo;
import de.ingrid.elasticsearch.IndexManager;
import de.ingrid.elasticsearch.QueryBuilderService;
import de.ingrid.elasticsearch.search.converter.QueryConverter;
import de.ingrid.utils.*;
import de.ingrid.utils.dsc.Column;
import de.ingrid.utils.dsc.Record;
import de.ingrid.utils.query.IngridQuery;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.stream.Stream;

@Component
public class IndexImpl implements ISearcher, IDetailer, IRecordLoader {

    private static final Logger log = LogManager.getLogger( IndexImpl.class );

    private final QueryBuilderService queryBuilderService;

    private final ElasticConfig config;

    private QueryConverter queryConverter;

    private FacetConverter facetConverter;

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
                Arrays.stream( new String[] {
                        PlugDescription.PARTNER,
                        PlugDescription.PROVIDER,
                        "datatype",
                        PlugDescription.DATA_SOURCE_NAME } ),
                Arrays.stream( config.additionalSearchDetailFields ) )
            .toArray(String[]::new);

        try {
            this.queryConverter = qc;
            this.facetConverter = fc;

            if (!config.esCommunicationThroughIBus) {
                log.info( "Elastic Search Settings: " + indexManager.printSettings() );
            }

        } catch (Exception e) {
            log.error( "Error during initialization of ElasticSearch-Client!", e );
        }

    }

    @SuppressWarnings("rawtypes")
    @Override
    public IngridHits search(IngridQuery ingridQuery, int startHit, int num) {

        // convert InGrid-query to QueryBuilder
        QueryBuilder query = queryConverter.convert( ingridQuery );

        QueryBuilder funcScoreQuery = null;
        if (config.indexEnableBoost) {
            funcScoreQuery = queryConverter.addScoreModifier( query );
        }

        boolean isLocationSearch = containsBoundingBox(ingridQuery);
        boolean hasFacets = ingridQuery.containsKey( "FACETS" );

        // request grouping information from index if necessary
        // see IndexImpl.getHitsFromResponse for usage
        String groupedBy = ingridQuery.getGrouped();
        String[] fields = null;
        if (IngridQuery.GROUPED_BY_PARTNER.equalsIgnoreCase( groupedBy )) {
            fields = new String[] { IngridQuery.PARTNER };
        } else if (IngridQuery.GROUPED_BY_ORGANISATION.equalsIgnoreCase( groupedBy )) {
            fields = new String[] { IngridQuery.PROVIDER };
        }/* else if (IngridQuery.GROUPED_BY_DATASOURCE.equalsIgnoreCase( groupedBy )) {
            // the necessary value id the results ID
        }*/

        IndexInfo[] indexInfos = this.config.activeIndices;

        if (indexInfos.length == 0) {
            log.debug( "No configured index to search on!" );
            return new IngridHits( 0, new IngridHit[0] );
        }

        // if we are remotely connected to an elasticsearch node then get the real indices of the aliases
        // otherwise we also get the results from other indices, since an alias can contain several indices!
        List<String> realIndices = new ArrayList<>();
        for (IndexInfo indexInfo : indexInfos) {
            String realIndex = indexManager.getIndexNameFromAliasName(
                    indexInfo.getToAlias(),
                    indexInfo.getRealIndexName() == null ? indexInfo.getToAlias() : indexInfo.getRealIndexName() );

            if (realIndex != null && !realIndices.contains(realIndex)) {
                realIndices.add( realIndex );
            }
        }
        String[] realIndexNames = realIndices.toArray( new String[0] );

        BoolQueryBuilder indexTypeFilter = queryBuilderService.createIndexTypeFilter( indexInfos );

        // Filter for results only with location information
        if (isLocationSearch) {
            BoolQueryBuilder boolShould = QueryBuilders.boolQuery();
            boolShould.must().add(QueryBuilders.existsQuery( "x1" ));
            indexTypeFilter.filter().add( boolShould );
        }

        // search prepare
        SearchRequestBuilder srb = indexManager.getClient().prepareSearch( realIndexNames  )
                // .setQuery( config.indexEnableBoost ? funcScoreQuery : query ) // Query
                .setQuery( config.indexEnableBoost
                        ? QueryBuilders.boolQuery().must( funcScoreQuery ).must( indexTypeFilter )
                        : QueryBuilders.boolQuery().must( query ).must( indexTypeFilter ) ) // Query
                .storedFields("iPlugId")
                .setFrom( startHit ).setSize( num ).setExplain( false );

        // Add sort by date to ES query if appropriate
        String rankingType = ingridQuery.getRankingType();
        if (rankingType != null && rankingType.equals( IngridQuery.DATE_RANKED )) {
            srb.addSort(SortBuilders
                    .fieldSort("modified")
                    .order(SortOrder.DESC)
                    .unmappedType("date"));
            srb.addSort(SortBuilders
                    .fieldSort("sort_hash")
                    .order(SortOrder.ASC)
                    .missing("_last")
                    .unmappedType("keyword"));
        } else {
            srb.addSort(SortBuilders
                    .scoreSort()
                    .order(SortOrder.DESC));
            srb.addSort(SortBuilders
                    .fieldSort("sort_hash")
                    .order(SortOrder.ASC)
                    .missing("_last")
                    .unmappedType("keyword"));
        }

        if (fields == null) {
            srb = srb.setFetchSource( false );
        } else {
            srb = srb.storedFields( fields );
        }

        // pre-processing: add facets/aggregations to the query
        if (hasFacets) {
            List<AbstractAggregationBuilder> aggregations = facetConverter.getAggregations( ingridQuery );
            for (AbstractAggregationBuilder aggregation : aggregations) {
                srb.addAggregation( aggregation );
            }
        }

        if(config.trackTotalHits) {
            srb.setTrackTotalHits(true);
        }

        if (log.isDebugEnabled()) {
            log.debug( "Final Elastic Search Query: \n" + srb );
        }

        // search!
        try {
            SearchResponse searchResponse = srb.execute().actionGet();

            // convert to IngridHits
            IngridHits hits = getHitsFromResponse( searchResponse, ingridQuery );

            // post-processing: extract and convert facets to InGrid-Document
            if (hasFacets) {
                // add facets from response
                IngridDocument facets = facetConverter.convertFacetResultsToDoc( searchResponse );
                hits.put( "FACETS", facets );
            }

            return hits;
        } catch (SearchPhaseExecutionException ex) {
            log.error( "Search failed on indices: " + Arrays.toString(realIndexNames), ex );
            return new IngridHits( 0, new IngridHit[0] );
        }
    }

    private boolean containsBoundingBox(IngridQuery ingridQuery) {
        boolean found = ingridQuery.containsField( "x1" );

        // also try to look in clauses
        if (!found) {
            for (IngridQuery clause : ingridQuery.getAllClauses()) {
                if (clause.containsField( "x1" )) {
                    return true;
                }
            }
        }
        return found;
    }

    /**
     * Create InGrid hits from ES hits. Add grouping information.
     */
    private IngridHits getHitsFromResponse(SearchResponse searchResponse, IngridQuery ingridQuery) {
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            log.error( "Error searching in index: " + failure.reason() );
        }

        SearchHits hits = searchResponse.getHits();

        // the size will not be bigger than it was requested in the query with
        // 'num'
        // so we can convert from long to int here!
        int length = hits.getHits().length;
        long totalHits = hits.getTotalHits().value;
        IngridHit[] hitArray = new IngridHit[length];
        int pos = 0;

        if (log.isDebugEnabled()) {
            log.debug( "Received " + length + " from " + totalHits + " hits." );
        }

        String groupBy = ingridQuery.getGrouped();
        for (SearchHit hit : hits.getHits()) {
            IngridHit ingridHit = new IngridHit( hit.field("iPlugId").getValue(), hit.getId(), -1, hit.getScore() );
            ingridHit.put( ELASTIC_SEARCH_INDEX, hit.getIndex() );
            ingridHit.put( ELASTIC_SEARCH_INDEX_TYPE, hit.getType() );

            // get grouing information, add if exist
            String groupValue = null;
            if (IngridQuery.GROUPED_BY_PARTNER.equalsIgnoreCase( groupBy )) {
                DocumentField field = hit.field(IngridQuery.PARTNER);
                if (field != null) {
                    groupValue = field.getValue().toString();
                }
            } else if (IngridQuery.GROUPED_BY_ORGANISATION.equalsIgnoreCase( groupBy )) {
                DocumentField field = hit.field( IngridQuery.PROVIDER );
                if (field != null) {
                    groupValue = field.getValue().toString();
                }
            } else if (IngridQuery.GROUPED_BY_DATASOURCE.equalsIgnoreCase( groupBy )) {
                groupValue = config.communicationProxyUrl;
                if (config.groupByUrl) {
                    try {
                        groupValue = new URL( hit.getId() ).getHost();
                    } catch (MalformedURLException e) {
                        log.warn( "can not group url: " + groupValue, e );
                    }
                }
            }
            if (groupValue != null) {
                ingridHit.addGroupedField( groupValue );
            }

            hitArray[pos] = ingridHit;
            pos++;
        }

        return new IngridHits((int) totalHits, hitArray );
    }

    @Override
    public IngridHitDetail getDetail(IngridHit hit, IngridQuery ingridQuery, String[] requestedFields) {
        for (int i = 0; i < requestedFields.length; i++) {
            requestedFields[i] = requestedFields[i].toLowerCase();
        }
        String documentId = hit.getDocumentId();
        String fromIndex = hit.getString( ELASTIC_SEARCH_INDEX );
        String fromType = hit.getString( ELASTIC_SEARCH_INDEX_TYPE );
        String[] allFields = Stream
                .concat( Arrays.stream( detailFields ), Arrays.stream( requestedFields ) )
                .filter(Objects::nonNull)
                .toArray(String[]::new);

        // We have to search here again, to get a highlighted summary of the result!
        QueryBuilder query = QueryBuilders.boolQuery().must( QueryBuilders.matchQuery( IngridDocument.DOCUMENT_UID, documentId ) ).must( queryConverter.convert( ingridQuery ) );

        // search prepare
        SearchRequestBuilder srb = indexManager.getClient().prepareSearch( fromIndex )
                .setFetchSource(true)
                .setQuery( query ) // Query
                .setFrom( 0 )
                .setSize( 1 )
                .storedFields( allFields )
                .setExplain( false );

        if(Arrays.asList(allFields).contains(config.indexFieldSummary)) {
            srb = srb.highlighter( new HighlightBuilder().field(config.indexFieldSummary+"*") );
        }

        SearchResponse searchResponse = srb.execute().actionGet();

        SearchHits dHits = searchResponse.getHits();
        return createDetail(hit, dHits.getAt( 0 ), allFields);
    }
    
    private IngridHitDetail createDetail(IngridHit hit, SearchHit dHit, String[] requestedFields) {

        String title = "untitled";
        if (dHit.field( config.indexFieldTitle ) != null) {
            title = dHit.field( config.indexFieldTitle ).getValue();
        }
        String summary = "";
        // try to get the summary first from the highlighted fields
        if (dHit.getHighlightFields().keySet().stream().anyMatch(k -> k.startsWith(config.indexFieldSummary))) {
            List<String> stringFragments = new ArrayList<>();
            for (Text fragment : dHit.getHighlightFields().entrySet().stream().filter(e -> e.getKey().startsWith(config.indexFieldSummary) ).findAny().get().getValue().fragments()) {
                stringFragments.add( fragment.toString() );
            }
            summary = String.join( " ... ", stringFragments );
            // otherwise get it from the original field
        } else if (dHit.field( config.indexFieldSummary ) != null) {
            summary = dHit.field( config.indexFieldSummary ).getValue();
        }

        IngridHitDetail detail = new IngridHitDetail( hit, title, summary );

        DocumentField dataSourceName = dHit.field(PlugDescription.DATA_SOURCE_NAME);

        if (dataSourceName == null) {
            log.error("The field dataSourceName could not be fetched from search index. This index field has to be stored! " +
                    "Check the index mapping file of the component.");
            throw new RuntimeException("DataSourceName not found in SearchHit. Possibly wrong mapping where index field is not stored.");
        }

        detail.setDataSourceName( dataSourceName.getValue().toString() );
        detail.setArray( "datatype", getStringArrayFromSearchHit( dHit, "datatype" ) );
        detail.setArray( PlugDescription.PARTNER, getStringArrayFromSearchHit( dHit, PlugDescription.PARTNER ) );
        detail.setArray( PlugDescription.PROVIDER, getStringArrayFromSearchHit( dHit, PlugDescription.PROVIDER ) );

        detail.setDocumentId( hit.getDocumentId() );
        for (String field : requestedFields) {
            if(detail.get(field) == null) {
                if (dHit.field(field) != null) {
                    if (dHit.field(field).getValues() != null) {
                        if (dHit.field(field).getValues().size() > 1) {
                            detail.put(field, dHit.field(field).getValues());
                        } else {
                            if (dHit.field(field).getValue() instanceof String) {
                                detail.put(field, new String[]{dHit.field(field).getValue()});
                            } else {
                                detail.put(field, dHit.field(field).getValue());
                            }
                        }
                    } else if (dHit.field(field).getValue() instanceof String) {
                        detail.put(field, new String[]{dHit.field(field).getValue()});
                    } else {
                        detail.put(field, dHit.field(field).getValue());
                    }
                }
            }
        }

        // add additional fields to detail object (such as url for iPlugSE)
        for (String extraDetail : config.additionalSearchDetailFields) {
            DocumentField field = dHit.getFields().get( extraDetail );
            if (field != null) {
                detail.put( extraDetail, field.getValue() );
            }
        }

        return detail;
    }

    private String[] getStringArrayFromSearchHit(SearchHit hit, String field) {
        DocumentField fieldObj = hit.field( field );
        if (fieldObj == null) {
            log.warn( "SearchHit does not contain field: " + field );
            return new String[0];
        }

        return fieldObj.getValues().toArray(new String[0]);
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
            details.add( getDetail( hit, ingridQuery, requestedFields ) );
        }
        return details.toArray( new IngridHitDetail[0] );
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
        ElasticDocument document = indexManager.getDocById( documentId );
        String[] fields = document.keySet().toArray( new String[0] );
        Record record = new Record();
        for (String name : fields) {
            Object stringValue = document.get( name );
            if (stringValue instanceof List) {
                for (String item : (List<String>) stringValue) {
                    Column column = new Column( null, name, null, true );
                    column.setTargetName( name );
                    record.addColumn( column, item );
                }
            } else {
                Column column = new Column( null, name, null, true );
                column.setTargetName( name );
                record.addColumn( column, stringValue );
            }
        }
        return record;
    }

}
