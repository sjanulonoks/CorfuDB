package org.corfudb.universe.scenario;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.UniverseFactory;
import org.corfudb.universe.group.CorfuCluster;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.LocalCorfuClient;
import org.corfudb.universe.scenario.fixture.Fixtures.CorfuClientFixture;
import org.corfudb.universe.scenario.fixture.Fixtures.UniverseFixture;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.Universe.UniverseParams;
import org.corfudb.util.Sleep;
import org.junit.After;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.group.CorfuCluster.CorfuClusterParams;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TIMEOUT;

public class HandOfGodIT {
    private static final UniverseFactory UNIVERSE_FACTORY = UniverseFactory.getInstance();

    private final DockerClient docker;
    private Universe universe;

    public HandOfGodIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @After
    public void tearDown() {
        if (universe != null) {
            universe.shutdown();
        }
    }

    /**
     * Test cluster behavior after killing and force removing nodes
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Kill two nodes
     * 3) Force remove the dead nodes (Hand of God)
     * 4) Verify layout and data path
     */
    @Test(timeout = 300000)
    public void HandOfGodTest() {
        // Deploy a default three nodes cluster
        UniverseFixture universeFixture = UniverseFixture.builder().build();
        CorfuClientFixture clientFixture = universeFixture.getGroup().getClient();

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

            testCase.it("Should force remove two nodes from cluster", data -> {
                CorfuServer server0 = corfuCluster.getNode("node9000");
                CorfuServer server1 = corfuCluster.getNode("node9001");
                CorfuServer server2 = corfuCluster.getNode("node9002");

                // Kill two nodes.  After each stop action,
                // wait for failure detector to work and cluster to stabilize
                server1.kill();
                Sleep.sleepUninterruptibly(Duration.ofSeconds(DEFAULT_TIMEOUT));
                server2.kill();
                Sleep.sleepUninterruptibly(Duration.ofSeconds(DEFAULT_TIMEOUT));

                // Force remove the dead nodes and wait for cluster to stabilize
                corfuClient.getManagementView().forceRemoveNode(
                        server1.getParams().getEndpoint(),
                        clientFixture.getNumRetry(),
                        clientFixture.getTimeout(),
                        clientFixture.getPollPeriod()
                );
                Sleep.sleepUninterruptibly(Duration.ofSeconds(DEFAULT_TIMEOUT));

                corfuClient.getManagementView().forceRemoveNode(
                        server2.getParams().getEndpoint(),
                        clientFixture.getNumRetry(),
                        clientFixture.getTimeout(),
                        clientFixture.getPollPeriod()
                );

                Sleep.sleepUninterruptibly(Duration.ofSeconds(DEFAULT_TIMEOUT));

                // Verify layout contains only one node
                corfuClient.invalidateLayout();
                Layout layout = corfuClient.getLayout();
                assertThat(layout.getAllActiveServers()).containsExactly(server0.getParams().getEndpoint());

                // Verify data path working
                for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                    assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
                }
            });
        });
    }
}
