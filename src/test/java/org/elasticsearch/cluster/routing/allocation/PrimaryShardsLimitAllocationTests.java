/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.PrimaryShardsLimitAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.junit.Test;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class PrimaryShardsLimitAllocationTests extends ElasticsearchAllocationTestCase {

    private final ESLogger logger = Loggers.getLogger(PrimaryShardsLimitAllocationTests.class);

    @Test
    public void indexLevelPrimaryShardsLimitAllocate() {
        AllocationService strategy = createAllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(ImmutableSettings.settingsBuilder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 8)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(PrimaryShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE, 2)))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();
        logger.info("Adding four nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2")).put(newNode("node3")).put(newNode("node4"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(clusterState.readOnlyRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(2));
        assertThat(clusterState.readOnlyRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(2));
        assertThat(clusterState.readOnlyRoutingNodes().node("node3").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(2));
        assertThat(clusterState.readOnlyRoutingNodes().node("node4").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(2));

        logger.info("Start the primary shards");
        RoutingNodes routingNodes = clusterState.routingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        int numOfPrimaries = 0;
        RoutingNode node = null;

        numOfPrimaries = 0;
        node = clusterState.readOnlyRoutingNodes().node("node1");
        for (int i = 0; i < node.size(); i ++) {
            if (node.get(i).primary()) {
                numOfPrimaries++;
            }
        }
        assertThat(numOfPrimaries, equalTo(2));
        numOfPrimaries = 0;
        node = clusterState.readOnlyRoutingNodes().node("node2");
        for (int i = 0; i < node.size(); i ++) {
            if (node.get(i).primary()) {
                numOfPrimaries++;
            }
        }
        assertThat(numOfPrimaries, equalTo(2));
        numOfPrimaries = 0;
        node = clusterState.readOnlyRoutingNodes().node("node3");
        for (int i = 0; i < node.size(); i ++) {
            if (node.get(i).primary()) {
                numOfPrimaries++;
            }
        }
        assertThat(numOfPrimaries, equalTo(2));
        numOfPrimaries = 0;
        node = clusterState.readOnlyRoutingNodes().node("node4");
        for (int i = 0; i < node.size(); i ++) {
            if (node.get(i).primary()) {
                numOfPrimaries++;
            }
        }
        assertThat(numOfPrimaries, equalTo(2));

        logger.info("Do another reroute, make sure its still not allocated");
        routingNodes = clusterState.routingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
    }
}
