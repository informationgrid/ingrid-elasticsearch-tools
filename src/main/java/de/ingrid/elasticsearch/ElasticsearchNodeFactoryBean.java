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
package de.ingrid.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportUtils;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class ElasticsearchNodeFactoryBean implements FactoryBean<ElasticsearchClient>,
        InitializingBean, DisposableBean {

    protected final Log log = LogFactory.getLog(getClass());

    private ElasticConfig config;

    private ElasticsearchClient client = null;

    @Autowired
    public void init(ElasticConfig config) {
        this.config = config;
    }

    public void afterPropertiesSet() throws Exception {
        // only setup elastic nodes if indexing is enabled
        if (config.isEnabled) {
            if (!config.esCommunicationThroughIBus) {
                if (log.isDebugEnabled()) {
                    log.debug("Elasticsearch: creating transport client: " + String.join(", ", config.remoteHosts));
                }
                createTransportClient(config);
            }
        } else {
            log.warn("Since Indexing is not enabled, this component should not have Elastic Search enabled at all! This bean should be excluded in the spring configuration.");
        }
    }

    public ElasticsearchClient getClient() {
        return client;
    }

    public void createTransportClient(ElasticConfig config) throws IOException {
        if (this.client != null) {
            client.shutdown();
        }

        List<HttpHost> hosts = new ArrayList<>();
        for (String host : config.remoteHosts) {
            hosts.add(HttpHost.create(host));
        }

        final CredentialsProvider credentialsProvider = getCredentialsProvider(config);

        SSLContext sslContext;
        if ("true".equals(config.sslTransport)) {
            Path caCertificatePath = Paths.get("elasticsearch-ca.pem");
            sslContext = TransportUtils.sslContextFromHttpCaCrt(caCertificatePath.toFile());
        } else {
            sslContext = null;
        }

        // Create the low-level client
        SSLContext finalSslContext = sslContext;
        RestClient restClient = RestClient
                .builder(hosts.toArray(new HttpHost[0]))
                .setHttpClientConfigCallback(httpClientBuilder -> {
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            httpClientBuilder.setSSLContext(finalSslContext);
                            return httpClientBuilder;
                        }
                )
                .build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // And create the API client
        client = new ElasticsearchClient(transport);
    }

    private static CredentialsProvider getCredentialsProvider(ElasticConfig config) {
        if (config.username != null && !config.username.isEmpty() && config.password != null && !config.password.isEmpty()) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(config.username, config.password));
            return credentialsProvider;
        } else return null;
    }


    public void destroy() {
        try {
            if (client != null)
                client.shutdown();
        } catch (final Exception e) {
            log.error("Error closing Elasticsearch node: ", e);
        }
    }

    @Override
    public ElasticsearchClient getObject() {
        return client;
    }

    @Override
    public Class<?> getObjectType() {
        return ElasticsearchClient.class;
    }

    public boolean isSingleton() {
        return true;
    }

}
