/*
 * **************************************************-
 * ingrid-iplug-se-iplug
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
package de.ingrid.elasticsearch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * A {@link FactoryBean} implementation used to create a {@link Node} element which is an embedded instance of the cluster within a running
 * application.
 * <p>
 * The lifecycle of the underlying {@link Node} instance is tied to the lifecycle of the bean via the {@link #destroy()} method which calls
 * {@link Node#close()}
 *
 * @author Erez Mazor (erezmazor@gmail.com)
 */
@Service
public class ElasticsearchNodeFactoryBean implements FactoryBean<Node>,
        InitializingBean, DisposableBean {

    protected final Log log = LogFactory.getLog(getClass());

    private ElasticConfig config;

    private Node node = null;

    private TransportClient client = null;

    @Autowired
    public void init(ElasticConfig config) {
        this.config = config;
    }

    public void afterPropertiesSet() throws Exception {
        // only setup elastic nodes if indexing is enabled
        if (config.isEnabled) {
            if (config.esCommunicationThroughIBus) {
                // do not initialize Elasticsearch since we use central index!

            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Elasticsearch: creating transport client: " + String.join(", ", config.remoteHosts));
                }
                createTransportClient(config);
            }
        } else {
            log.warn("Since Indexing is not enabled, this component should not have Elastic Search enabled at all! This bean should be excluded in the spring configuration.");
        }
    }

    public Client getClient() {
        return client;
    }

    public void createTransportClient(ElasticConfig config) throws UnknownHostException {

        /*
         * The following commented code will be used for the new Client in Elasticsearch 7!?
         */

        /*RestHighLevelClient transportClient = null;

        List<HttpHost> addresses = new ArrayList<>();
        for (String host : esRemoteHosts) {
            String[] splittedHost = host.split( ":" );
            addresses.add(new HttpHost(splittedHost[0], Integer.valueOf(splittedHost[1]), "http"));
        }

        transportClient = new RestHighLevelClient(RestClient.builder(addresses.toArray(new HttpHost[0])));*/

        /*Properties props = getPropertiesFromElasticsearch();
        if (props != null) {
            transportClient = new RestHighLevelClient( Settings.builder().putProperties( props ).build() );
        } else {
        }*/

        // Version 6
        if (this.client != null) {
            for (TransportAddress addr : this.client.transportAddresses()) {
                this.client.removeTransportAddress(addr);
            }
        }

        client = new PreBuiltXPackTransportClient(Settings.builder()
                .put("cluster.name", config.clusterName)
                .put("xpack.security.user", config.username + ":" + config.password)
                .build());

        for (String host : config.remoteHosts) {
            String[] splittedHost = host.split(":");
            this.client.addTransportAddress(new TransportAddress(InetAddress.getByName(splittedHost[0]), Integer.parseInt(splittedHost[1])));
        }
    }


    public void destroy() {
        try {
            if (client != null)
                client.close();
            if (node != null)
                node.close();
        } catch (final Exception e) {
            log.error("Error closing Elasticsearch node: ", e);
        }
    }

    /**
     * Elasticsearch does not allow creating local nodes. Get the client directly instead.
     */
    @Deprecated
    public Node getObject() throws Exception {
        int cnt = 1;
        while (node == null && cnt <= 10) {
            log.info("Wait for elastic search node to start: " + cnt + " sec.");
            Thread.sleep(1000);
            cnt++;
        }
        if (node == null) {
            log.error("Could not start Elastic Search node within 10 sec!");
            throw new RuntimeException("Could not start Elastic Search node within 10 sec!");
        }
        return node;
    }

    public Class<Node> getObjectType() {
        return Node.class;
    }

    public boolean isSingleton() {
        return true;
    }

}
