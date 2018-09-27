package org.corfudb.universe.scenario;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.UniverseFactory;
import org.corfudb.universe.group.CorfuCluster;
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
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TIMEOUT_MEDIUM;
import static org.corfudb.universe.universe.Universe.UniverseParams;

public class OneNodePausedIT {
    private static final UniverseFactory UNIVERSE_FACTORY = UniverseFactory.getInstance();

    private final DockerClient docker;
    private Universe universe;

    public OneNodePausedIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @After
    public void tearDown() {
        if (universe != null) {
            universe.shutdown();
        }
    }

    /**
     * Test cluster behavior after one node paused
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Pause one node (hang the jvm process)
     * 3) Verify layout, cluster status and data path
     * 4) Recover cluster by resuming the paused node
     * 5) Verify layout, cluster status and data path again
     */
    @Test(timeout = 300000)
    public void OneNodePausedTest() {
        // Deploy a default three nodes cluster
        UniverseFixture universeFixture = UniverseFixture.builder().build();

        universe = UNIVERSE_FACTORY.buildDockerUniverse(universeFixture.data(), docker).deploy();

        Scenario<UniverseParams, UniverseFixture> scenario = Scenario.with(universeFixture);

        scenario.describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(universeFixture.getGroup().getGroupName());

            CorfuCluster.CorfuClusterParams corfuClusterParams = corfuCluster.getParams();
            LocalCorfuClient corfuClient = corfuCluster.getLocalCorfuClient(corfuClusterParams.getServers());

            CorfuTable table = corfuClient.createDefaultCorfuTable(DEFAULT_STREAM_NAME);
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            testCase.it("Should pause one node and then resume", data -> {
                CorfuServer server1 = corfuCluster.getNode("node9001");

                // Pause one node
                server1.pause();
                // Wait for failure detector to work and cluster to stabilize. This takes
                // longer than the stop/kill case since the network channel is not broken,
                // and the failure detector will get connection timeout exception
                Sleep.sleepUninterruptibly(Duration.ofSeconds(DEFAULT_TIMEOUT_MEDIUM));

                // Verify layout, unresponsive servers should contain only one node
                corfuClient.invalidateLayout();
                Layout layout = corfuClient.getLayout();
                assertThat(layout.getUnresponsiveServers()).containsExactly(server1.getParams().getEndpoint());

                // Verify cluster status is DEGRADED with one node down
                ClusterStatusReport clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.DEGRADED);
                Map<String, NodeStatus> statusMap = clusterStatusReport.getClientServerConnectivityStatusMap();
                assertThat(statusMap.get(server1.getParams().getEndpoint())).isEqualTo(NodeStatus.DOWN);

                // Verify data path working fine
                for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                    assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
                }

                // Recover cluster by resuming the paused node
                server1.resume();
                Sleep.sleepUninterruptibly(Duration.ofSeconds(DEFAULT_TIMEOUT_MEDIUM));

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
