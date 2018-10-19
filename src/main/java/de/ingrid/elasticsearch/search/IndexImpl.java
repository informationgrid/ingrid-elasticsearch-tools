/*
 * **************************************************-
 * ingrid-iplug-se-iplug
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
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Component
public class IndexImpl implements ISearcher, IDetailer, IRecordLoader {

    public static final String DETAIL_URL = "url";

    private static Logger log = Logger.getLogger( IndexImpl.class );
    
    private QueryBuilderService queryBuilderService;

    private ElasticConfig config;

    private QueryConverter queryConverter;

    private FacetConverter facetConverter;

    private static final String ELASTIC_SEARCH_INDEX = "es_index";

    private static final String ELASTIC_SEARCH_INDEX_TYPE = "es_type";

    private String[] detailFields;

    private IndexManager indexManager;

    public List<String> indexSearchInTypes = new ArrayList<>();

    @Autowired
    public IndexImpl(ElasticConfig config, IndexManager indexManager, QueryConverter qc, FacetConverter fc, QueryBuilderService queryBuilderService) {
        this.indexManager = indexManager;
        this.config = config;
        this.queryBuilderService = queryBuilderService;
        
        detailFields = Stream.concat( 
                Arrays.stream( new String[] { 
                        config.indexFieldTitle, 
                        config.indexFieldSummary, 
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
        String[] instances = getSearchInstances( ingridQuery );

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
            log.warn( "No configured index to search on!" );
            return new IngridHits( 0, new IngridHit[0] );
        }
        
        // if we are remotely connected to an elasticsearch node then get the real indices of the aliases
        // otherwise we also get the results from other indices, since an alias can contain several indices!
        List<String> realIndices = new ArrayList<>();
        for (IndexInfo indexInfo : indexInfos) {
            String realIndex = indexManager.getIndexNameFromAliasName( 
                    indexInfo.getToAlias(), 
                    indexInfo.getRealIndexName() == null ? indexInfo.getToAlias() : indexInfo.getRealIndexName() );
            
            if (realIndex != null) {
                realIndices.add( realIndex );
            }
        }
        String[] realIndexNames = realIndices.toArray( new String[0] );
        
        BoolQueryBuilder indexTypeFilter = queryBuilderService.createIndexTypeFilter( indexInfos );
        
        // search prepare
        SearchRequestBuilder srb = indexManager.getClient().prepareSearch( realIndexNames  )
                // .setQuery( config.indexEnableBoost ? funcScoreQuery : query ) // Query
                .setQuery( config.indexEnableBoost 
                        ? QueryBuilders.boolQuery().must( funcScoreQuery ).must( indexTypeFilter )
                        : QueryBuilders.boolQuery().must( query ).must( indexTypeFilter ) ) // Query
                .setFrom( startHit ).setSize( num ).setExplain( false );

        // search only in defined types within the index, if defined
        if (instances.length > 0) {
            srb.setTypes( instances );
        }

        if (fields == null) {
            srb = srb.setFetchSource( false );
        } else {
            srb = srb.storedFields( fields );
        }

        // Filter for results only with location information
        if (isLocationSearch) {
            srb.setPostFilter( QueryBuilders.existsQuery( "x1" ) );
        }

        // pre-processing: add facets/aggregations to the query
        if (hasFacets) {
            List<AbstractAggregationBuilder> aggregations = facetConverter.getAggregations( ingridQuery );
            for (AbstractAggregationBuilder aggregation : aggregations) {
                srb.addAggregation( aggregation );
            }
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
            log.error( "Search failed on indices: " + realIndexNames, ex );
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
     * Check first the query for a hidden field which contains the information of the instances to search in for. If there's none, then use
     * the defined one in the configuration. The parameter in the query should be only used for an internal search within the iPlug.
     * 
     * @param ingridQuery
     * @return
     */
    private String[] getSearchInstances(IngridQuery ingridQuery) {
        String[] instances = (String[]) ingridQuery.getArray( "searchInInstances" );
        if (instances == null || instances.length == 0) {
            instances = this.indexSearchInTypes.toArray( new String[0] );
        }
        return instances;
    }

    /**
     * Create InGrid hits from ES hits. Add grouping information.
     * 
     * @param searchResponse
     * @param ingridQuery
     * @return
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
        int totalHits = (int) hits.getTotalHits();
        IngridHit[] hitArray = new IngridHit[length];
        int pos = 0;
        
        if (log.isDebugEnabled()) {
            log.debug( "Received " + length + " from " + totalHits + " hits." );
        }

        String groupBy = ingridQuery.getGrouped();
        for (SearchHit hit : hits.getHits()) {
            IngridHit ingridHit = new IngridHit( config.communicationProxyUrl, hit.getId(), -1, hit.getScore() );
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

        return new IngridHits( totalHits, hitArray );
    }

    @Override
    public IngridHitDetail getDetail(IngridHit hit, IngridQuery ingridQuery, String[] requestedFields) {
        for (int i = 0; i < requestedFields.length; i++) {
            requestedFields[i] = requestedFields[i].toLowerCase();
        }
        String documentId = hit.getDocumentId();
        String fromIndex = hit.getString( ELASTIC_SEARCH_INDEX );
        String fromType = hit.getString( ELASTIC_SEARCH_INDEX_TYPE );
        String[] allFields = Stream.concat( Arrays.stream( detailFields ), Arrays.stream( requestedFields ) ).toArray(String[]::new);

        // We have to search here again, to get a highlighted summary of the result!
        QueryBuilder query = QueryBuilders.boolQuery().must( QueryBuilders.matchQuery( IngridDocument.DOCUMENT_UID, documentId ) ).must( queryConverter.convert( ingridQuery ) );
        
        // search prepare
        SearchRequestBuilder srb = indexManager.getClient().prepareSearch( fromIndex ).setTypes( fromType )
                .setFetchSource(true)
                .setQuery( query ) // Query
                .setFrom( 0 )
                .setSize( 1 )
                .highlighter( new HighlightBuilder().field(config.indexFieldSummary) )
                .storedFields( allFields )
                .setExplain( false );

        SearchResponse searchResponse = srb.execute().actionGet();

        SearchHits dHits = searchResponse.getHits();
        SearchHit dHit = dHits.getAt( 0 );

        String title = "untitled";
        if (dHit.field( config.indexFieldTitle ) != null) {
            title = dHit.field( config.indexFieldTitle ).getValue();
        }
        String summary = "";
        // try to get the summary first from the highlighted fields
        if (dHit.getHighlightFields().containsKey( config.indexFieldSummary )) {
            List<String> stringFragments = new ArrayList<>();
            for (Text fragment : dHit.getHighlightFields().get( config.indexFieldSummary ).fragments()) {
                stringFragments.add( fragment.toString() );
            }
            summary = String.join( " ... ", stringFragments );
            // otherwise get it from the original field
        } else if (dHit.field( config.indexFieldSummary ) != null) {
            summary = dHit.field( config.indexFieldSummary ).getValue();
        }

        IngridHitDetail detail = new IngridHitDetail( hit, title, summary );

        detail.setDataSourceName( dHit.field( PlugDescription.DATA_SOURCE_NAME ).getValue().toString() );
        detail.setArray( "datatype", getStringArrayFromSearchHit( dHit, "datatype" ) );
        detail.setArray( PlugDescription.PARTNER, getStringArrayFromSearchHit( dHit, PlugDescription.PARTNER ) );
        detail.setArray( PlugDescription.PROVIDER, getStringArrayFromSearchHit( dHit, PlugDescription.PROVIDER ) );

        detail.setDocumentId( documentId );
        for (String field : requestedFields) {
            if (dHit.field( field ) != null) {
                if (dHit.field(field).getValues() != null){
                    if(dHit.field( field ).getValues().size() > 1){
                        detail.put( field, dHit.field( field ).getValues());
                    }else{
                        if (dHit.field( field ).getValue() instanceof String) {
                            detail.put( field, new String[] { dHit.field( field ).getValue() } );
                        } else {
                            detail.put( field, dHit.field( field ).getValue() );
                        }
                    }
                } else if (dHit.field( field ).getValue() instanceof String) {
                    detail.put( field, new String[] { dHit.field( field ).getValue() } );
                } else {
                    detail.put( field, dHit.field( field ).getValue() );
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

    public ElasticDocument getDocById(Object id) {
        String idAsString = String.valueOf( id );
        IndexInfo[] indexNames = this.config.activeIndices;
        // iterate over all indices until document was found
        for (IndexInfo indexName : indexNames) {
            try {
                Map<String, Object> source = indexManager.getClient().prepareGet( indexName.getToAlias(), null, idAsString )
                        .setFetchSource( config.indexFieldsIncluded, config.indexFieldsExcluded )
                        .execute().actionGet().getSource();
                
                if (source != null) {
                    return new ElasticDocument( source );
                }
            } catch(IndexNotFoundException ex) {
                log.warn( "Index was not found. We probably have to clean up or refresh the active indices here. Missing index is: " + indexName.getToAlias() );
            }
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Record getRecord(IngridHit hit) {
        String documentId = hit.getDocumentId();
        ElasticDocument document = getDocById( documentId );
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
