/*-
 * **************************************************-
 * InGrid Elasticsearch Tools
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
package de.ingrid.elasticsearch;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import de.ingrid.utils.ElasticDocument;
import de.ingrid.utils.PlugDescription;
import org.json.simple.JSONObject;

public interface IIndexManager {

    String getIndexNameFromAliasName(String indexAlias, String partialName);

    boolean createIndex(String name);

    boolean createIndex(String name, String esMapping, String esSettings);

    void switchAlias(String aliasName, String oldIndex, String newIndex);

    void checkAndCreateInformationIndex();

    String getIndexTypeIdentifier(IndexInfo indexInfo);

    void update(IndexInfo indexinfo, ElasticDocument doc, boolean updateOldIndex);

    void updatePlugDescription(PlugDescription plugDescription) throws IOException;

    void updateIPlugInformation(String id, JSONObject info) throws InterruptedException, ExecutionException;

    void flush();

    void deleteIndex(String index);

    String[] getIndices(String filter);

    Map<String, Object> getMapping(IndexInfo indexInfo);

    String getDefaultMapping();

    String getDefaultSettings();

    void updateHearbeatInformation(Map<String, Object> iPlugIdInfos) throws InterruptedException, ExecutionException, IOException;

    void delete(IndexInfo indexinfo, String id, boolean updateOldIndex);

    boolean indexExists(String indexName);
}
