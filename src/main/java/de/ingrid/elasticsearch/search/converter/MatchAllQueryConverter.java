package de.ingrid.elasticsearch.search.converter;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import de.ingrid.elasticsearch.search.IQueryParsers;
import de.ingrid.utils.query.IngridQuery;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

@Service
@Order(100)
public class MatchAllQueryConverter implements IQueryParsers {

    @Override
    public void parse(IngridQuery ingridQuery, BoolQuery.Builder queryBuilder) {
        // NOTICE: is also called on sub clauses BUT WE ONLY PROCESS THE TOP INGRID QUERY.
        // all other ones are subclasses !
        boolean isTopQuery = ingridQuery.getClass().equals(IngridQuery.class);
        boolean hasTerms = ingridQuery.getTerms().length > 0;
        if (!hasTerms && isTopQuery && !queryBuilder.hasClauses()) {
            BoolQuery.Builder bq = new BoolQuery.Builder();
            bq.must(QueryBuilders.matchAll().build()._toQuery());
            queryBuilder.must(bq.build()._toQuery());
        }
    }
}
