/*
 * **************************************************-
 * ingrid-base-webapp
 * ==================================================
 * Copyright (C) 2014 - 2026 wemove digital solutions GmbH
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
package de.ingrid.elasticsearch;

import java.io.Serializable;

public class IndexInfo implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = -2290409004042430234L;
    
    private String toIndex;
    
    @Deprecated
    private String toType;
    private String toAlias = null;
    private String docIdField;
    
    private String realIndexName;
    
    public String getToIndex() {
        return toIndex;
    }
    public void setToIndex(String toIndex) {
        this.toIndex = toIndex;
    }
    @Deprecated
    public String getToType() {
        return toType;
    }
    @Deprecated
    public void setToType(String toType) {
        this.toType = toType;
    }
    public String getDocIdField() {
        return docIdField;
    }
    public void setDocIdField(String docIdField) {
        this.docIdField = docIdField;
    }
    public String getRealIndexName() {
        if (realIndexName == null) {
            return toIndex;
        }
        return realIndexName;
    }
    public void setRealIndexName(String realIndexName) {
        this.realIndexName = realIndexName;
    }
    
    /**
     * Create a copy of this object.
     */
    public IndexInfo clone() {
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.setDocIdField( docIdField );
        indexInfo.setRealIndexName( realIndexName );
        indexInfo.setToIndex( toIndex );
        indexInfo.setToType( toType );
        return indexInfo;
    }
    
    public String getIdentifier() {
        return getToIndex() + "." + getToType();
    }
    public String getToAlias() {
        return (toAlias == null) ? toIndex : toAlias;
    }
    public void setToAlias(String toAlias) {
        this.toAlias = toAlias;
    }
}
