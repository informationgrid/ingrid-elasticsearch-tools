/*
 * **************************************************-
 * ingrid-iplug-se-iplug
 * ==================================================
 * Copyright (C) 2014 - 2025 wemove digital solutions GmbH
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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.json.JsonData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import de.ingrid.elasticsearch.search.IQueryParsers;
import de.ingrid.utils.query.FieldQuery;
import de.ingrid.utils.query.IngridQuery;

/**
 * This class is a specialized field converter, which transforms special fields
 * for date and location into a correct query.
 *
 * @author André
 *
 */
@Service
@Order(3)
public class FieldQueryIGCConverter implements IQueryParsers {

    private final static Logger log = LogManager.getLogger(FieldQueryIGCConverter.class);

    @Override
    @SuppressWarnings("unchecked")
    public BoolQuery.Builder parse(IngridQuery ingridQuery, BoolQuery.Builder queryBuilder) {
        FieldQuery[] fields = ingridQuery.getFields();

        Map<String, Object> geoMap = new HashMap<>(fields.length);
        Map<String, Object> timeMap = new HashMap<>(fields.length);

        BoolQuery.Builder bqBuilder = null;

        for (FieldQuery fieldQuery : fields) {
            Query subQuery;

            String indexField = fieldQuery.getFieldName();
            String value = fieldQuery.getFieldValue().toLowerCase();

            switch (indexField) {
                case "x1":
                case "x2":
                case "y1":
                case "y2":
                    geoMap.put(indexField, value);
                    break;
                case "coord":
                    List<String> geoList = (List<String>) geoMap.getOrDefault(indexField, new LinkedList<>());
                    geoList.add(value);
                    geoMap.put(indexField, geoList);
                    break;
                case "t0":
                case "t1":
                case "t2":
                    timeMap.put(indexField, value);
                    break;
                case "time":
                    List<String> timeList = (List<String>) timeMap.getOrDefault(indexField, new LinkedList<>());
                    timeList.add(value);
                    timeMap.put(indexField, timeList);
                    break;
                case "incl_meta":
                    if ("on".equals(value)) {
                        // Implement the required logic for incl_meta field if needed
                    }
                    break;
                default:
                    if (value.contains("*")) {
                        subQuery = QueryBuilders.wildcard(w -> w.field(indexField).value(value));
                    } else {
                        subQuery = QueryBuilders.match(m -> m.field(fieldQuery.getFieldName()).query(fieldQuery.getFieldValue()));
                    }
                    bqBuilder = ConverterUtils.applyAndOrRules(fieldQuery, bqBuilder, subQuery);
            }
        }

        if (bqBuilder != null) {
            if (fields[0].isRequred()) {
                queryBuilder.must(bqBuilder.build()._toQuery());
            } else {
                queryBuilder.should(bqBuilder.build()._toQuery());
            }
        }

        if (geoMap.get("coord") == null) {
            List<String> list = new LinkedList<>();
            list.add("exact");
            geoMap.put("coord", list);
        }

        prepareGeo(queryBuilder, geoMap);
        prepareTime(queryBuilder, timeMap);
        return queryBuilder;
    }

    @SuppressWarnings("unchecked")
    private void prepareGeo(BoolQuery.Builder queryBuilder, Map<String, Object> geoMap) {
        List<String> list = (List<String>) geoMap.get("coord");
        if (list != null) {
            for (String value : list) {
                switch (value) {
                    case "inside":
                        prepareInsideGeoQuery(queryBuilder, geoMap);
                        break;
                    case "intersect":
                        prepareIntersectGeoQuery(queryBuilder, geoMap);
                        break;
                    case "include":
                        prepareIncludeGeoQuery(queryBuilder, geoMap);
                        break;
                    default:
                        prepareExactGeoQuery(queryBuilder, geoMap);
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("resulting query:" + queryBuilder.toString());
        }
    }

    private static void prepareIncludeGeoQuery(BoolQuery.Builder queryBuilder, Map<String, Object> geoMap) {
        String x1 = (String) geoMap.get("x1");
        String x2 = (String) geoMap.get("x2");
        String y1 = (String) geoMap.get("y1");
        String y2 = (String) geoMap.get("y2");

        if (x1 != null && x2 != null && y1 != null && y2 != null) {
            queryBuilder.must(QueryBuilders.range(r -> r.field("x1").gte(JsonData.of(-180.0)).lte(JsonData.of(Double.valueOf(x1)))))
                    .must(QueryBuilders.range(r -> r.field("x2").gte(JsonData.of(Double.valueOf(x2))).lte(JsonData.of(180.0))))
                    .must(QueryBuilders.range(r -> r.field("y1").gte(JsonData.of(-180.0)).lte(JsonData.of(Double.valueOf(y1)))))
                    .must(QueryBuilders.range(r -> r.field("y2").gte(JsonData.of(Double.valueOf(y2))).lte(JsonData.of(180.0))));
        }
    }

    private static void prepareExactGeoQuery(BoolQuery.Builder queryBuilder, Map<String, Object> geoMap) {
        String x1 = (String) geoMap.get("x1");
        String x2 = (String) geoMap.get("x2");
        String y1 = (String) geoMap.get("y1");
        String y2 = (String) geoMap.get("y2");

        if (x1 != null && x2 != null && y1 != null && y2 != null) {
            queryBuilder.must(QueryBuilders.term(t -> t.field("x1").value(Double.valueOf(x1))))
                    .must(QueryBuilders.term(t -> t.field("x2").value(Double.valueOf(x2))))
                    .must(QueryBuilders.term(t -> t.field("y1").value(Double.valueOf(y1))))
                    .must(QueryBuilders.term(t -> t.field("y2").value(Double.valueOf(y2))));
        }
    }

    private static void prepareIntersectGeoQuery(BoolQuery.Builder queryBuilder, Map<String, Object> geoMap) {
        String x1 = (String) geoMap.get("x1");
        String x2 = (String) geoMap.get("x2");
        String y1 = (String) geoMap.get("y1");
        String y2 = (String) geoMap.get("y2");

        if (x1 != null && x2 != null && y1 != null && y2 != null) {
            queryBuilder.must(QueryBuilders.bool(b -> b
                    .should(QueryBuilders.range(r -> r.field("x1").gte(JsonData.of(Double.valueOf(x1))).lte(JsonData.of(Double.valueOf(x2)))))
                    .should(QueryBuilders.range(r -> r.field("y1").gte(JsonData.of(Double.valueOf(y1))).lte(JsonData.of(Double.valueOf(y2)))))
                    .should(QueryBuilders.range(r -> r.field("x2").gte(JsonData.of(Double.valueOf(x1))).lte(JsonData.of(Double.valueOf(x2)))))
                    .should(QueryBuilders.range(r -> r.field("y2").gte(JsonData.of(Double.valueOf(y1))).lte(JsonData.of(Double.valueOf(y2))))))
            );

            queryBuilder.must(QueryBuilders.bool(b -> b
                    .should(QueryBuilders.range(r -> r.field("x1").gte(JsonData.of(-180.0)).lte(JsonData.of(Double.valueOf(x1)))))
                    .should(QueryBuilders.range(r -> r.field("x2").gte(JsonData.of(Double.valueOf(x2))).lte(JsonData.of(180.0))))
                    .should(QueryBuilders.range(r -> r.field("y1").gte(JsonData.of(-180.0)).lte(JsonData.of(Double.valueOf(y1)))))
                    .should(QueryBuilders.range(r -> r.field("y2").gte(JsonData.of(Double.valueOf(y2))).lte(JsonData.of(180.0)))))
            );

            queryBuilder.must(QueryBuilders.range(r -> r.field("x1").gte(JsonData.of(-180.0)).lte(JsonData.of(Double.valueOf(x2)))))
                    .must(QueryBuilders.range(r -> r.field("x2").gte(JsonData.of(Double.valueOf(x1))).lte(JsonData.of(180.0))))
                    .must(QueryBuilders.range(r -> r.field("y1").gte(JsonData.of(-180.0)).lte(JsonData.of(Double.valueOf(y2)))))
                    .must(QueryBuilders.range(r -> r.field("y2").gte(JsonData.of(Double.valueOf(y1))).lte(JsonData.of(180.0))));
        }
    }

    private static void prepareInsideGeoQuery(BoolQuery.Builder queryBuilder, Map<String, Object> geoMap) {
        String x1 = (String) geoMap.get("x1");
        String x2 = (String) geoMap.get("x2");
        String y1 = (String) geoMap.get("y1");
        String y2 = (String) geoMap.get("y2");

        if (x1 != null && x2 != null && y1 != null && y2 != null) {
            queryBuilder.mustNot(QueryBuilders.range(r -> r.field("x1").gte(JsonData.of(-180.0)).lt(JsonData.of(Double.valueOf(x1)))))
                    .mustNot(QueryBuilders.range(r -> r.field("x2").gt(JsonData.of(Double.valueOf(x2))).lte(JsonData.of(180.0))))
                    .mustNot(QueryBuilders.range(r -> r.field("y1").gte(JsonData.of(-180.0)).lt(JsonData.of(Double.valueOf(y1)))))
                    .mustNot(QueryBuilders.range(r -> r.field("y2").gt(JsonData.of(Double.valueOf(y2))).lte(JsonData.of(180.0))));
        }
    }

    @SuppressWarnings("unchecked")
    private static void prepareTime(BoolQuery.Builder queryBuilder, Map<String, Object> timeMap) {
        if (log.isDebugEnabled()) {
            log.debug("start prepareTime with t0=" + timeMap.get("t0") + ", t1:" + timeMap.get("t1") + ", t2:" + timeMap.get("t2"));
        }


                                                List<String> list = (List<String>) timeMap.get("time");
    if (list == null) {
        prepareInsideTime(queryBuilder, timeMap);
    } else {
        for (String value : list) {
            switch (value) {
                case "intersect":
                    prepareInsideOrIntersectTime(queryBuilder, timeMap);
                    break;
                case "include":
                    prepareInsideOrIncludeTime(queryBuilder, timeMap);
                    break;
                default:
                    prepareInsideTime(queryBuilder, timeMap);
            }
        }
    }

    if (log.isDebugEnabled()) {
        log.debug("resulting query:" + queryBuilder.toString());
    }
}

private static void prepareInsideOrIncludeTime(BoolQuery.Builder queryBuilder, Map<String, Object> timeMap) {
    BoolQuery.Builder booleanQueryTime = new BoolQuery.Builder();
    BoolQuery.Builder inside = new BoolQuery.Builder();
    BoolQuery.Builder include = new BoolQuery.Builder();

    prepareInsideTime(inside, timeMap);
    prepareIncludeTimeQuery(include, timeMap);

    if (include.hasClauses()) {
        booleanQueryTime.should(include.build()._toQuery());
    }
    if (inside.hasClauses()) {
        booleanQueryTime.should(inside.build()._toQuery());
    }

    if (booleanQueryTime.hasClauses()) {
        queryBuilder.must(booleanQueryTime.build()._toQuery());
    }
}

private static void prepareInsideOrIntersectTime(BoolQuery.Builder queryBuilder, Map<String, Object> timeMap) {
    BoolQuery.Builder booleanQueryTime = new BoolQuery.Builder();
    BoolQuery.Builder inside = new BoolQuery.Builder();
    BoolQuery.Builder traverse = new BoolQuery.Builder();

    prepareInsideTime(inside, timeMap);
    if (inside.hasClauses()) {
        booleanQueryTime.should(inside.build()._toQuery());
    }
    prepareTraverseTime(traverse, timeMap);
    if (traverse.hasClauses()) {
        booleanQueryTime.should(traverse.build()._toQuery());
    }
    if (booleanQueryTime.hasClauses()) {
        queryBuilder.must(booleanQueryTime.build()._toQuery());
    }
}

private static void prepareInsideTime(BoolQuery.Builder queryBuilder, Map<String, Object> timeMap) {
    String t0 = (String) timeMap.get("t0");
    String t1 = (String) timeMap.get("t1");
    String t2 = (String) timeMap.get("t2");

    if (t1 != null && t2 != null) {
        queryBuilder.should(QueryBuilders.range(r -> r.field("t0").gte(JsonData.of(t1)).lte(JsonData.of(t2))))
                .should(QueryBuilders.bool(b -> b
                        .must(QueryBuilders.range(r -> r.field("t1").gte(JsonData.of(t1)).lte(JsonData.of(t2))))
                        .must(QueryBuilders.range(r -> r.field("t2").gte(JsonData.of(t1)).lte(JsonData.of(t2))))));
    } else if (t0 != null) {
        queryBuilder.must(QueryBuilders.range(r -> r.field("t0").gte(JsonData.of(t0)).lte(JsonData.of(t0))));
    }
}

private static void prepareIncludeTimeQuery(BoolQuery.Builder queryBuilder, Map<String, Object> timeMap) {
    String t0 = (String) timeMap.get("t0");
    String t1 = (String) timeMap.get("t1");
    String t2 = (String) timeMap.get("t2");

    if (t1 != null && t2 != null) {
        queryBuilder.must(QueryBuilders.range(r -> r.field("t1").lte(JsonData.of(t1))))
                .must(QueryBuilders.range(r -> r.field("t2").gte(JsonData.of(t2))));
    } else if (t0 != null) {
        queryBuilder.must(QueryBuilders.range(r -> r.field("t1").lte(JsonData.of(t0))))
                .must(QueryBuilders.range(r -> r.field("t2").gte(JsonData.of(t0))));
    }
}

private static void prepareTraverseTime(BoolQuery.Builder queryBuilder, Map<String, Object> timeMap) {
    String t0 = (String) timeMap.get("t0");
    String t1 = (String) timeMap.get("t1");
    String t2 = (String) timeMap.get("t2");

    if (t1 != null && t2 != null) {
        queryBuilder.must(QueryBuilders.bool(b -> b
                .should(QueryBuilders.bool(sb -> sb
                        .must(QueryBuilders.range(r -> r.field("t1").lte(JsonData.of(t1))))
                        .must(QueryBuilders.range(r -> r.field("t2").gte(JsonData.of(t1)).lte(JsonData.of(t2))))))
                .should(QueryBuilders.bool(sb -> sb
                        .must(QueryBuilders.range(r -> r.field("t1").gte(JsonData.of(t1)).lte(JsonData.of(t2))))
                        .must(QueryBuilders.range(r -> r.field("t2").gte(JsonData.of(t2))))))));
    } else if (t0 != null) {
        queryBuilder.must(QueryBuilders.bool(b -> b
                .should(QueryBuilders.term(t -> t.field("t0").value(t0)))
                .should(QueryBuilders.term(t -> t.field("t1").value(t0)))
                .should(QueryBuilders.term(t -> t.field("t2").value(t0)))));
    }
}
}
