package de.ingrid.elasticsearch.search.converter;

import co.elastic.clients.elasticsearch._types.query_dsl.*;
import de.ingrid.elasticsearch.ElasticConfig;
import de.ingrid.elasticsearch.search.IQueryParsers;
import de.ingrid.utils.query.IngridQuery;
import de.ingrid.utils.query.TermQuery;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Order(1)
public class DefaultFieldsQueryConverter implements IQueryParsers {

    private static final Logger log = LogManager.getLogger(DefaultFieldsQueryConverter.class);

    private Map<String, Float> fieldBoosts;

    @Autowired
    public DefaultFieldsQueryConverter(ElasticConfig config) {
        fieldBoosts = getFieldBoostMap(config.indexSearchDefaultFields);
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

    @Override
    public BoolQuery.Builder parse(IngridQuery ingridQuery, BoolQuery.Builder queryBuilder) {
        TermQuery[] terms = ingridQuery.getTerms();

        BoolQuery.Builder bq = new BoolQuery.Builder();

        if (terms.length > 0) {
            List<String> termsAnd = new ArrayList<>();
            List<String> termsOr = new ArrayList<>();
            for (TermQuery term : terms) {
                String t = term.getTerm();
                Query subQuery = null;

                // if it's a phrase
                if (t.contains(" ")) {
                    BoolQuery.Builder phraseQuery = new BoolQuery.Builder();
                    for (Map.Entry<String, Float> field : fieldBoosts.entrySet()) {
                        phraseQuery.should(QueryBuilders.matchPhrase(m -> m.field(field.getKey()).query(t).boost(field.getValue())));
                    }
                    subQuery = phraseQuery.build()._toQuery();
                } else if (t.contains("*")) {
                    subQuery = QueryBuilders.queryString(q -> q.query(t));
                } else if (term.isProhibited()) {
                    subQuery = QueryBuilders.multiMatch(m -> m.query(t).fields(List.of(fieldBoosts.keySet().toArray(new String[0]))));
                } else {
                    if (term.isRequred()) {
                        termsAnd.add(t);
                    } else {
                        termsOr.add(t);
                    }
                    continue;
                }

                if (term.isRequred()) {
                    if (term.isProhibited()) {
                        bq.mustNot(subQuery);
                    } else {
                        bq.must(subQuery);
                    }
                } else {
                    BoolQuery.Builder parentBq = new BoolQuery.Builder();
                    parentBq.should(bq.build()._toQuery()).should(subQuery);
                    bq = parentBq;
                }
            }

            if (!termsAnd.isEmpty()) {
                String join = String.join(" ", termsAnd);
                MultiMatchQuery subQuery = MultiMatchQuery.of(m -> m
                        .query(join)
                        .fields(List.of(fieldBoosts.keySet().toArray(new String[0])))
                        .operator(Operator.And)
                        .type(TextQueryType.CrossFields));
                bq.should(subQuery._toQuery());
            }
            if (!termsOr.isEmpty()) {
                String join = String.join(" ", termsOr);
                MultiMatchQuery subQuery = MultiMatchQuery.of(m -> m
                        .query(join)
                        .fields(List.of(fieldBoosts.keySet().toArray(new String[0])))
                        .operator(Operator.Or)
                        .type(TextQueryType.CrossFields));
                bq.should(subQuery._toQuery());
            }

            if (bq != null) {
                if (terms.length > 0 && terms[0].isRequred()) {
                    queryBuilder.must(bq.build()._toQuery());
                } else {
                    queryBuilder.should(bq.build()._toQuery());
                }
            }
        }
        return queryBuilder;
    }
}
