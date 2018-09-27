package org.corfudb.universe.scenario;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.corfudb.universe.UniverseFactory;
import org.corfudb.universe.group.CorfuCluster;
import org.corfudb.universe.group.CorfuCluster.CorfuClusterParams;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.CorfuServer.ServerParams;
import org.corfudb.universe.node.LocalCorfuClient;
import org.corfudb.universe.scenario.fixture.Fixtures.UniverseFixture;
import org.corfudb.universe.universe.Universe;
import org.corfudb.util.Sleep;
import org.junit.After;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst;
import static org.corfudb.universe.universe.Universe.UniverseParams;

public class AllNodesPartitionedIT {
    private static final UniverseFactory UNIVERSE_FACTORY = UniverseFactory.getInstance();

    private final DockerClient docker;
    private Universe universe;

    public AllNodesPartitionedIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @After
    public void tearDown() {
        if (universe != null) {
            universe.shutdown();
        }
    }

    /**
     * Test cluster behavior after all nodes are partitioned symmetrically
     *
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Symmetrically partition all nodes so that they can't communicate
     *    to any other node in cluster and vice versa
     * 3) Verify layout, cluster status and data path
     * 4) Recover cluster by reconnecting the partitioned node
     * 5) Verify layout, cluster status and data path again
     */
    @Test(timeout = 600000)
    public void AllNodesPartitionedTest() {
        UniverseFixture universeFixture = UniverseFixture.builder().build();

        universe = UNIVERSE_FACTORY
                .buildDockerUniverse(universeFixture.data(), docker)
                .deploy();

        Scenario<UniverseParams, UniverseFixture> scenario = Scenario.with(universeFixture);

        scenario.describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(universeFixture.getGroup().getGroupName());

            CorfuClusterParams clusterParams = corfuCluster.getParams();
            LocalCorfuClient corfuClient = corfuCluster.getLocalCorfuClient(clusterParams.getServers());

            CorfuTable table = corfuClient.createDefaultCorfuTable(TestFixtureConst.DEFAULT_STREAM_NAME);
            for (int i = 0; i < TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            testCase.it("Should partition all nodes and then recover", data -> {
                // Symmetrically partition all nodes and wait for failure
                // detector to work and cluster to stabilize
                corfuCluster.<CorfuServer>nodes().values().forEach(node -> {
                    node.disconnect();
                    Sleep.sleepUninterruptibly(Duration.ofSeconds(TestFixtureConst.DEFAULT_TIMEOUT_LONG));
                });

                Sleep.sleepUninterruptibly(Duration.ofSeconds(TestFixtureConst.DEFAULT_TIMEOUT));

                // Verify cluster status is UNAVAILABLE with all nodes UP
                corfuClient.invalidateLayout();
                ClusterStatusReport clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.UNAVAILABLE);

                Map<String, NodeStatus> statusMap = clusterStatusReport.getClientServerConnectivityStatusMap();

                corfuCluster.<CorfuServer>nodes().values().forEach(node -> {
                    ServerParams serverParams = node.getParams();
                    assertThat(statusMap.get(serverParams.getEndpoint())).isEqualTo(NodeStatus.UP);
                });

                // Recover cluster by removing partitions and wait for cluster to stabilize
                corfuCluster.<CorfuServer>nodes().values().forEach(node -> {
                    node.reconnect();
                    Sleep.sleepUninterruptibly(Duration.ofSeconds(TestFixtureConst.DEFAULT_TIMEOUT_LONG));
                });

                Sleep.sleepUninterruptibly(Duration.ofSeconds(TestFixtureConst.DEFAULT_TIMEOUT));

                // Verify cluster status is STABLE
                corfuClient.invalidateLayout();
                clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.STABLE);

                // Verify data path working fine
                for (int i = 0; i < TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
                    assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
                }
            });
        });
    }
}
