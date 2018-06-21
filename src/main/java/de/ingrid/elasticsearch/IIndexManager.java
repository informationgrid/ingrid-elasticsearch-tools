/*-
 * **************************************************-
 * InGrid Elasticsearch Tools
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
package de.ingrid.elasticsearch;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import de.ingrid.utils.ElasticDocument;

public interface IIndexManager {

    public String getIndexNameFromAliasName(String indexAlias, String partialName);
    
    public boolean createIndex(String name);
    
    public boolean createIndex(String name, String type, String source);
    
    public void switchAlias(String aliasName, String oldIndex, String newIndex);
    
    public void checkAndCreateInformationIndex();
    
    public String getIndexTypeIdentifier(IndexInfo indexInfo);
    
    public void update(IndexInfo indexinfo, ElasticDocument doc, boolean updateOldIndex);
    
    public void updateIPlugInformation(String id, String info) throws InterruptedException, ExecutionException;
    
    public void flush();
    
    public void deleteIndex(String index);
    
    public Map<String, Object> getMapping(IndexInfo indexInfo);
    
    public void updateHearbeatInformation(Map<String, String> iPlugIdInfos) throws InterruptedException, ExecutionException, IOException;
    
    public void delete(IndexInfo indexinfo, String id, boolean updateOldIndex);
}
