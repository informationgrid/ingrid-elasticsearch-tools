package de.ingrid.elasticsearch.search.converter;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import de.ingrid.elasticsearch.search.IQueryParsers;
import de.ingrid.utils.query.IngridQuery;
import de.ingrid.utils.query.TermQuery;
import org.springframework.stereotype.Service;

@Service
public class GlobalQueryConverter implements IQueryParsers {

    @Override
    public void parse(IngridQuery ingridQuery, BoolQuery.Builder queryBuilder) {
        TermQuery[] terms = ingridQuery.getTerms();

        BoolQuery.Builder bq = new BoolQuery.Builder();

        if (terms.length == 0) {
            bq.must(QueryBuilders.matchAll().build()._toQuery());
            queryBuilder.must(bq.build()._toQuery());
        } else {
            for (TermQuery term : terms) {
                Query termQuery = QueryBuilders.queryString(q -> q.query(term.getTerm()));

                if (term.isRequred()) {
                    bq.must(termQuery);
                } else {
                    bq.should(termQuery);
                }
            }

            if (terms[0].isRequred()) {
                queryBuilder.must(bq.build()._toQuery());
            } else {
                queryBuilder.should(bq.build()._toQuery());
            }
        }
    }
}
