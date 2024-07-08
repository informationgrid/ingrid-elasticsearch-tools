/*-
 * **************************************************-
 * InGrid Elasticsearch Tools
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

import co.elastic.clients.elasticsearch._types.query_dsl.*;
import de.ingrid.elasticsearch.ElasticConfig;
import de.ingrid.elasticsearch.search.IQueryParsers;
import de.ingrid.utils.query.ClauseQuery;
import de.ingrid.utils.query.IngridQuery;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class QueryConverter implements IQueryParsers {

    private static final Logger log = LogManager.getLogger(QueryConverter.class);

    @Autowired
    private List<IQueryParsers> _queryConverter;

    @Autowired
    private ElasticConfig _config;

    private Map<String, Float> fieldBoosts;

    public QueryConverter() {
        _queryConverter = new ArrayList<>();
        fieldBoosts = getFieldBoostMap(new String[]{"title^10", "summary^2", "content"});
    }

    public void setQueryParsers(List<IQueryParsers> parsers) {
        this._queryConverter = parsers;
    }

    public BoolQuery.Builder convert(IngridQuery ingridQuery) {
        BoolQuery.Builder qb = new BoolQuery.Builder();

        ClauseQuery[] clauses = ingridQuery.getClauses();
        for (ClauseQuery clauseQuery : clauses) {
            final BoolQuery.Builder res = convert(clauseQuery);
            if (clauseQuery.isRequred()) {
                if (clauseQuery.isProhibited())
                    qb.mustNot(res.build()._toQuery());
                else
                    qb.must(res.build()._toQuery());
            } else {
                qb.should(res.build()._toQuery());
            }
        }
        return parse(ingridQuery, qb);
    }

    public BoolQuery.Builder parse(IngridQuery ingridQuery, BoolQuery.Builder booleanQuery) {
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
            BoolQuery.Builder originSubQuery = new BoolQuery.Builder();
            for (Map.Entry<String, Float> field : fieldBoosts.entrySet()) {
                originSubQuery.should(QueryBuilders.matchPhrase(m -> m.field(field.getKey()).query(origin).boost(field.getValue())));
            }
            if (booleanQuery.hasClauses()) {
                return new BoolQuery.Builder().should(originSubQuery.build()._toQuery(), booleanQuery.build()._toQuery());
            }
        }
        return booleanQuery;
    }

    /**
     * Wrap a score modifier around the query, which uses a field from the document
     * to boost the score.
     *
     * @param query is the query to apply the score modifier on
     * @return a new query which contains the score modifier and the given query
     */
    public FunctionScoreQuery.Builder addScoreModifier(Query query) {

        // describe the function to manipulate the score
        FieldValueFactorScoreFunction scoreFunc = FieldValueFactorScoreFunction.of(f -> f
                .field(_config.boostField)
                .missing(1.0)
                .modifier(getModifier(_config.boostModifier))
                .factor((double) _config.boostFactor)
        );

        return QueryBuilders.functionScore()
                .query(query)
                .functions(FunctionScore.of(sf -> sf
                        .fieldValueFactor(scoreFunc)
                ))
                .boostMode(getBoostMode(_config.boostMode));

    }

    private FieldValueFactorModifier getModifier(String esBoostModifier) {
        FieldValueFactorModifier result;
        switch (esBoostModifier) {
            case "LN":
                result = FieldValueFactorModifier.Ln;
                break;
            case "LN1P":
                result = FieldValueFactorModifier.Ln1p;
                break;
            case "LN2P":
                result = FieldValueFactorModifier.Ln2p;
                break;
            case "LOG":
                result = FieldValueFactorModifier.Log;
                break;
            case "LOG1P":
                result = FieldValueFactorModifier.Log1p;
                break;
            case "LOG2P":
                result = FieldValueFactorModifier.Log2p;
                break;
            case "NONE":
                result = FieldValueFactorModifier.None;
                break;
            case "RECIPROCAL":
                result = FieldValueFactorModifier.Reciprocal;
                break;
            case "SQRT":
                result = FieldValueFactorModifier.Sqrt;
                break;
            case "SQUARE":
                result = FieldValueFactorModifier.Square;
                break;
            default:
                result = FieldValueFactorModifier.Log1p;
                break;
        }
        return result;
    }

    private FunctionBoostMode getBoostMode(String esBoostMode) {
        FunctionBoostMode result;
        switch (esBoostMode) {
            case "SUM":
                result = FunctionBoostMode.Sum;
                break;
            case "AVG":
                result = FunctionBoostMode.Avg;
                break;
            case "MAX":
                result = FunctionBoostMode.Max;
                break;
            case "MIN":
                result = FunctionBoostMode.Min;
                break;
            case "MULTIPLY":
                result = FunctionBoostMode.Multiply;
                break;
            case "REPLACE":
                result = FunctionBoostMode.Replace;
                break;
            default:
                result = FunctionBoostMode.Sum;
                break;
        }
        return result;
    }

    private Map<String, Float> getFieldBoostMap(String[] indexSearchDefaultFields) {
        Map<String, Float> result = new HashMap<>();
        for (String field : indexSearchDefaultFields) {
            if (field.contains("^")) {
                String[] split = field.split("\\^");
                result.put(split[0], Float.parseFloat(split[1]));
            } else {
                result.put(field, 1.0F);
            }
        }
        return result;
    }
}
