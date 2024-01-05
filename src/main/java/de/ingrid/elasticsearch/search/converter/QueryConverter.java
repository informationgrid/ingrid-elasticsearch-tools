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
package de.ingrid.elasticsearch.search.converter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction.Modifier;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import de.ingrid.elasticsearch.ElasticConfig;
import de.ingrid.elasticsearch.search.IQueryParsers;
import de.ingrid.utils.query.ClauseQuery;
import de.ingrid.utils.query.IngridQuery;

import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;

@Service
public class QueryConverter implements IQueryParsers {

    private static Logger log = LogManager.getLogger( QueryConverter.class );

    @Autowired
    private List<IQueryParsers> _queryConverter;

    @Autowired
    private ElasticConfig _config;

    private Map<String, Float> fieldBoosts;

    public QueryConverter() {
        _queryConverter = new ArrayList<>();
        fieldBoosts = getFieldBoostMap(new String[] {"title^10", "summary^2","content"});
    }

    public void setQueryParsers(List<IQueryParsers> parsers) {
        this._queryConverter = parsers;
    }

    public BoolQueryBuilder convert(IngridQuery ingridQuery) {

        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        ClauseQuery[] clauses = ingridQuery.getClauses();
        for (ClauseQuery clauseQuery : clauses) {
            final BoolQueryBuilder res = convert(clauseQuery);
            if (clauseQuery.isRequred()) {
                if (clauseQuery.isProhibited())
                    qb.mustNot( res );
                else
                    qb.must( res );
            } else {
                qb.should( res );
            }
        }
        parse(ingridQuery, qb);

        return qb;

    }

    public void parse(IngridQuery ingridQuery, BoolQueryBuilder booleanQuery) {
        if (log.isDebugEnabled()) {
            log.debug("incoming ingrid query:" + ingridQuery.toString());
        }
        for (IQueryParsers queryConverter : _queryConverter) {
            if (log.isDebugEnabled()) {
                log.debug("incoming boolean query:" + booleanQuery.toString());
            }
            queryConverter.parse(ingridQuery, booleanQuery);
            if (log.isDebugEnabled()) {
                log.debug(queryConverter.toString() + ": resulting boolean query:" + booleanQuery.toString());
            }
        }
        String origin = (String) ingridQuery.get(IngridQuery.ORIGIN);
        if (origin != null && !origin.isEmpty()) {
            BoolQueryBuilder originSubQuery = QueryBuilders.boolQuery();
            for (Map.Entry<String, Float> field : fieldBoosts.entrySet()) {
                originSubQuery.should( QueryBuilders.matchPhraseQuery( field.getKey(), origin ).boost(field.getValue()) );
            }
            if(booleanQuery.hasClauses()){
                BoolQueryBuilder subQuery = QueryBuilders.boolQuery();

                subQuery.should().addAll(booleanQuery.should());
                booleanQuery.should().clear();

                subQuery.must().addAll(booleanQuery.must());
                booleanQuery.must().clear();

                subQuery.mustNot().addAll(booleanQuery.mustNot());
                booleanQuery.mustNot().clear();

                subQuery.filter().addAll(booleanQuery.filter());
                booleanQuery.filter().clear();

                originSubQuery.should(subQuery);
            }
            booleanQuery.should(originSubQuery);
        }
    }

    /**
     * Wrap a score modifier around the query, which uses a field from the document
     * to boost the score.
     * @param query is the query to apply the score modifier on
     * @return a new query which contains the score modifier and the given query
     */
    public QueryBuilder addScoreModifier(QueryBuilder query) {

        // describe the function to manipulate the score
        FieldValueFactorFunctionBuilder scoreFunc = ScoreFunctionBuilders
            .fieldValueFactorFunction( _config.boostField )
            .missing(1.0)
            .modifier( getModifier(_config.boostModifier) )
            .factor( _config.boostFactor );

        return functionScoreQuery(query, scoreFunc)
                .boostMode( getBoostMode(_config.boostMode) );
    }

    private Modifier getModifier(String esBoostModifier) {
        Modifier result;
        switch (esBoostModifier) {
        case "LN":
            result = Modifier.LN;
            break;
        case "LN1P":
            result = Modifier.LN1P;
            break;
        case "LN2P":
            result = Modifier.LN2P;
            break;
        case "LOG":
            result = Modifier.LOG;
            break;
        case "LOG1P":
            result = Modifier.LOG1P;
            break;
        case "LOG2P":
            result = Modifier.LOG2P;
            break;
        case "NONE":
            result = Modifier.NONE;
            break;
        case "RECIPROCAL":
            result = Modifier.RECIPROCAL;
            break;
        case "SQRT":
            result = Modifier.SQRT;
            break;
        case "SQUARE":
            result = Modifier.SQUARE;
            break;

        default:
            result = Modifier.LOG1P;
            break;
        }
        return result;
    }

    private CombineFunction getBoostMode(String esBoostMode) {
        CombineFunction result;
        switch (esBoostMode) {
        case "SUM":
            result = CombineFunction.SUM;
            break;
        case "AVG":
            result = CombineFunction.AVG;
            break;
        case "MAX":
            result = CombineFunction.MAX;
            break;
        case "MIN":
            result = CombineFunction.MIN;
            break;
        case "MULTIPLY":
            result = CombineFunction.MULTIPLY;
            break;
        case "REPLACE":
            result = CombineFunction.REPLACE;
            break;
        default:
            result = CombineFunction.SUM;
            break;
        }
        return result;
    }

    private Map<String, Float> getFieldBoostMap(String[] indexSearchDefaultFields) {
        Map<String, Float> result = new HashMap<>();
        for (String field:indexSearchDefaultFields) {
            if(field.contains("^")){
                String[] split = field.split("\\^");
                result.put(split[0], Float.parseFloat(split[1]));
            }
            else{
                result.put(field, Float.valueOf(1.0F));
            }
        }
        return result;
    }
}
