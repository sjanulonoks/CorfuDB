package org.corfudb.universe.scenario;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.UniverseFactory;
import org.corfudb.universe.group.CorfuCluster;
import org.corfudb.universe.group.CorfuCluster.CorfuClusterParams;
import org.corfudb.universe.node.CorfuServer;
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
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.*;
import static org.corfudb.universe.universe.Universe.UniverseParams;

public class OneNodePartitionedIT {
    private static final UniverseFactory UNIVERSE_FACTORY = UniverseFactory.getInstance();

    private final DockerClient docker;
    private Universe universe;

    public OneNodePartitionedIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @After
    public void tearDown() {
        if (universe != null) {
            universe.shutdown();
        }
    }

    /**
     * Test cluster behavior after one node partitioned symmetrically
     *
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Symmetrically partition one node so that it can't communicate
     *    to any other node in cluster and vice versa
     * 3) Verify layout, cluster status and data path
     * 4) Recover cluster by reconnecting the partitioned node
     * 5) Verify layout, cluster status and data path again
     */
    @Test(timeout = 480000)
    public void OneNodeSymmetricPartitionTest() {
        // Deploy a default three nodes cluster
        UniverseFixture universeFixture = UniverseFixture.builder().build();

        universe = UNIVERSE_FACTORY.buildDockerUniverse(universeFixture.data(), docker).deploy();

        Scenario<UniverseParams, UniverseFixture> scenario = Scenario.with(universeFixture);

        scenario.describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(universeFixture.getGroup().getGroupName());

            CorfuClusterParams corfuClusterParams = corfuCluster.getParams();
            LocalCorfuClient corfuClient = corfuCluster.getLocalCorfuClient(corfuClusterParams.getServers());

            CorfuTable table = corfuClient.createDefaultCorfuTable(DEFAULT_STREAM_NAME);
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            testCase.it("should symmetrically partition one node from cluster", data -> {
                CorfuServer server1 = corfuCluster.getNode("node9001");

                // Symmetrically Partition one node and
                server1.disconnect();
                // Wait for failure detector to work and cluster to stabilize
                Sleep.sleepUninterruptibly(Duration.ofSeconds(DEFAULT_TIMEOUT_LONG));

                // Verify layout, unresponsive servers should contain only one node
                corfuClient.invalidateLayout();
                Layout layout = corfuClient.getLayout();
                assertThat(layout.getUnresponsiveServers()).containsExactly(server1.getParams().getEndpoint());

                // Verify cluster status is DEGRADED with all nodes UP
                ClusterStatusReport clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.DEGRADED);
                Map<String, NodeStatus> statusMap = clusterStatusReport.getClientServerConnectivityStatusMap();
                assertThat(statusMap.get(server1.getParams().getEndpoint())).isEqualTo(NodeStatus.UP);

                // Verify data path working fine
                for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                    assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
                }

                // Remove partition and wait for cluster to stabilize
                server1.reconnect();
                Sleep.sleepUninterruptibly(Duration.ofSeconds(DEFAULT_TIMEOUT_LONG));

                // Verify layout have no unresponsive servers
                corfuClient.invalidateLayout();
                assertThat(corfuClient.getLayout().getUnresponsiveServers().size()).isEqualTo(0);

                // Verify cluster status is STABLE
                clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.STABLE);

                // Verify data path working fine
                for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                    assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
                }
            });
        });
    }
}
