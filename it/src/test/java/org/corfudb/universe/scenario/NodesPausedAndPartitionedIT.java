package org.corfudb.universe.scenario;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.corfudb.universe.UniverseFactory;
import org.corfudb.universe.group.CorfuCluster;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.LocalCorfuClient;
import org.corfudb.universe.scenario.fixture.Fixtures.UniverseFixture;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.Universe.UniverseParams;
import org.corfudb.util.Sleep;
import org.junit.After;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst;

public class NodesPausedAndPartitionedIT {
    private static final UniverseFactory UNIVERSE_FACTORY = UniverseFactory.getInstance();

    private final DockerClient docker;
    private Universe universe;

    public NodesPausedAndPartitionedIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @After
    public void tearDown() {
        if (universe != null) {
            universe.shutdown();
        }
    }

    /**
     * Test cluster behavior after one paused and another node partitioned
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Pause one node
     * 3) Symmetrically partition one node
     * 4) Verify layout, cluster status and data path
     * 5) Recover cluster by restart the stopped node and fix partition
     * 5) Verify layout, cluster status and data path
     */
    @Test(timeout = 300000)
    public void NodesPausedAndPartitionedTest() {
        // Deploy a default three nodes cluster
        UniverseFixture universeFixture = UniverseFixture.builder().build();

        universe = UNIVERSE_FACTORY.buildDockerUniverse(universeFixture.data(), docker).deploy();

        Scenario<UniverseParams, UniverseFixture> scenario = Scenario.with(universeFixture);

        scenario.describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(universeFixture.getGroup().getGroupName());

            CorfuCluster.CorfuClusterParams corfuClusterParams = corfuCluster.getParams();
            LocalCorfuClient corfuClient = corfuCluster.getLocalCorfuClient(corfuClusterParams.getServers());

            CorfuTable table = corfuClient.createDefaultCorfuTable(TestFixtureConst.DEFAULT_STREAM_NAME);
            for (int i = 0; i < TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            testCase.it("Should pause one node and partition another", data -> {
                CorfuServer server0 = corfuCluster.getNode("node9000");
                CorfuServer server1 = corfuCluster.getNode("node9001");
                CorfuServer server2 = corfuCluster.getNode("node9002");

                // Pause one node and wait for failure detector to work and cluster to stabilize
                server1.pause();
                Sleep.sleepUninterruptibly(Duration.ofSeconds(TestFixtureConst.DEFAULT_TIMEOUT_MEDIUM));
                // Partition one node and wait for failure detector to work and cluster to stabilize
                server2.disconnect();
                Sleep.sleepUninterruptibly(Duration.ofSeconds(TestFixtureConst.DEFAULT_TIMEOUT_LONG));

                // Verify cluster status is UNAVAILABLE with two nodes up and one node down
                corfuClient.invalidateLayout();
                ClusterStatusReport clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.UNAVAILABLE);

                Map<String, NodeStatus> statusMap = clusterStatusReport.getClientServerConnectivityStatusMap();
                assertThat(statusMap.get(server0.getParams().getEndpoint())).isEqualTo(NodeStatus.UP);
                assertThat(statusMap.get(server1.getParams().getEndpoint())).isEqualTo(NodeStatus.DOWN);
                assertThat(statusMap.get(server2.getParams().getEndpoint())).isEqualTo(NodeStatus.UP);

                // Recover cluster by resuming the paused node, removing
                // partition and wait for cluster to stabilize
                server1.resume();
                Sleep.sleepUninterruptibly(Duration.ofSeconds(TestFixtureConst.DEFAULT_TIMEOUT));
                server2.reconnect();
                Sleep.sleepUninterruptibly(Duration.ofSeconds(TestFixtureConst.DEFAULT_TIMEOUT));

                // Verify layout have no unresponsive servers
                corfuClient.invalidateLayout();
                assertThat(corfuClient.getLayout().getUnresponsiveServers().size()).isEqualTo(0);

                // Verify cluster status is STABLE
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
