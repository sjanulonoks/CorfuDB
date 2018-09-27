package org.corfudb.universe.scenario;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.universe.UniverseFactory;
import org.corfudb.universe.group.CorfuCluster;
import org.corfudb.universe.group.CorfuCluster.CorfuClusterParams;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.LocalCorfuClient;
import org.corfudb.universe.universe.Universe;
import org.corfudb.util.Sleep;
import org.junit.After;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.fixture.Fixtures.CorfuClientFixture;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst;
import static org.corfudb.universe.scenario.fixture.Fixtures.UniverseFixture;
import static org.corfudb.universe.universe.Universe.UniverseParams;

public class ClusterResizeIT {
    private static final UniverseFactory UNIVERSE_FACTORY = UniverseFactory.getInstance();

    private final DockerClient docker;
    private Universe universe;

    public ClusterResizeIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @After
    public void tearDown() {
        if (universe != null) {
            universe.shutdown();
        }
    }

    /**
     * Test cluster behavior after add/remove nodes
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially remove two nodes from cluster
     * 3) Verify layout and data path
     * 4) Sequentially add two nodes back into cluster
     * 5) Verify layout and data path again
     */
    @Test(timeout = 300000)
    public void clusterResizeTest() {
        // Deploy a default three nodes cluster
        UniverseFixture universeFixture = UniverseFixture.builder().build();
        final int numNodes = universeFixture.getGroup().getServers().getNumNodes();
        CorfuClientFixture clientFixture = universeFixture.getGroup().getClient();

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

            testCase.it("should remove two nodes from corfu cluster", data -> {
                CorfuServer server0 = corfuCluster.getNode("node9000");

                // Remove two nodes from cluster and wait for cluster to stabilize
                for (int i = 1; i <= numNodes - 1; i++) {
                    CorfuServer candidate = corfuCluster.getNode("node" + (9000 + i));
                    corfuClient.getManagementView().removeNode(
                            candidate.getParams().getEndpoint(),
                            clientFixture.getNumRetry(),
                            clientFixture.getTimeout(),
                            clientFixture.getPollPeriod()
                    );
                }

                Sleep.sleepUninterruptibly(Duration.ofSeconds(TestFixtureConst.DEFAULT_TIMEOUT));

                // Verify layout contains only one node
                assertThat(corfuClient.getLayout().getAllServers()).containsExactly(server0.getParams().getEndpoint());

                // Verify data path working fine
                checkCorfuTable(table);
            });

            testCase.it("should add two nodes back to corfu cluster", data -> {
                // Add two nodes back into cluster and Wait for cluster to stabilize
                for (int i = 1; i <= numNodes - 1; i++) {
                    CorfuServer candidate = corfuCluster.getNode("node" + (9000 + i));
                    corfuClient.getManagementView().addNode(
                            candidate.getParams().getEndpoint(),
                            clientFixture.getNumRetry(),
                            clientFixture.getTimeout(),
                            clientFixture.getPollPeriod()
                    );
                }

                Sleep.sleepUninterruptibly(Duration.ofSeconds(TestFixtureConst.DEFAULT_TIMEOUT));

                // Verify layout should contain all three nodes
                assertThat(corfuClient.getLayout().getAllServers().size()).isEqualTo(corfuCluster.nodes().size());

                // Verify data path working fine
                checkCorfuTable(table);
            });
        });
    }

    private void checkCorfuTable(CorfuTable table) {
        for (int x = 0; x < TestFixtureConst.DEFAULT_TABLE_ITER; x++) {
            assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
        }
    }
}
