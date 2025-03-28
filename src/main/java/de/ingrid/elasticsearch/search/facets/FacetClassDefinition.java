/*
 * **************************************************-
 * ingrid-search-utils
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
package de.ingrid.elasticsearch.search.facets;

public class FacetClassDefinition {
    private String name;

    private String queryFragment;
    
    /**
     * contains the calculated document hits matching this Facet class
     */
    private long hitCount = -1;

    
    public FacetClassDefinition(String facetName, String queryFragment) {
        this.name = facetName;
        this.queryFragment = queryFragment;
    }

    public void setHitCount(long hitCount) {
        this.hitCount = hitCount;
    }

    public long getHitCount() {
        return hitCount;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setQueryFragment(String queryFragment) {
        this.queryFragment = queryFragment;
    }

    public String getFragment() {
        return queryFragment;
    }

}
