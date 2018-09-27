package org.corfudb.universe.scenario;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.universe.UniverseFactory;
import org.corfudb.universe.group.CorfuCluster;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.LocalCorfuClient;
import org.corfudb.universe.scenario.fixture.Fixtures.*;
import org.corfudb.universe.universe.Universe;
import org.corfudb.util.Sleep;
import org.junit.After;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.group.CorfuCluster.CorfuClusterParams;
import static org.corfudb.universe.universe.Universe.UniverseParams;

public class ConcurrentClusterResizeIT {
    private static final UniverseFactory UNIVERSE_FACTORY = UniverseFactory.getInstance();

    private final DockerClient docker;
    private Universe universe;

    public ConcurrentClusterResizeIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @After
    public void tearDown() {
        if (universe != null) {
            universe.shutdown();
        }
    }

    /**
     * Test cluster behavior after add/remove nodes concurrently
     * <p>
     * 1) Deploy and bootstrap a five nodes cluster
     * 2) Concurrently remove four nodes from cluster
     * 3) Verify layout and data path
     * 4) Concurrently add four nodes back into cluster
     * 5) Verify layout and data path again
     */
    @Test(timeout = 480000)
    public void concurrentClusterResizeTest() {
        // Deploy a five nodes cluster
        final int numNodes = 5;
        MultipleServersFixture serversFixture = MultipleServersFixture.builder().numNodes(numNodes).build();
        CorfuGroupFixture groupFixture = CorfuGroupFixture.builder().servers(serversFixture).build();
        UniverseFixture universeFixture = UniverseFixture.builder().group(groupFixture).build();
        CorfuClientFixture clientFixture = groupFixture.getClient();

        universe = UNIVERSE_FACTORY
                .buildDockerUniverse(universeFixture.data(), docker)
                .deploy();

        Scenario<UniverseParams, UniverseFixture> scenario = Scenario.with(universeFixture);

        scenario.describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(universeFixture.getGroup().getGroupName());

            CorfuClusterParams corfuClusterParams = corfuCluster.getParams();
            LocalCorfuClient corfuClient = corfuCluster.getLocalCorfuClient(corfuClusterParams.getServers());

            CorfuTable table = corfuClient.createDefaultCorfuTable(TestFixtureConst.DEFAULT_STREAM_NAME);
            for (int i = 0; i < TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            // Get the servers list to be added/removed
            List<CorfuServer> servers = IntStream.range(1, numNodes)
                    .mapToObj(i -> (CorfuServer) corfuCluster.getNode("node" + (9000 + i)))
                    .collect(Collectors.toList());

            testCase.it("should concurrently remove four nodes from cluster", data -> {
                CorfuServer server0 = corfuCluster.getNode("node9000");

                // Concurrently remove four nodes from cluster and wait for cluster to stabilize
                ExecutorService executor = Executors.newFixedThreadPool(numNodes - 1);

                servers.forEach(node -> executor.submit(() -> corfuClient.getManagementView().removeNode(
                        node.getParams().getEndpoint(),
                        clientFixture.getNumRetry(),
                        clientFixture.getTimeout(),
                        clientFixture.getPollPeriod())
                ));

                Sleep.sleepUninterruptibly(Duration.ofSeconds(TestFixtureConst.DEFAULT_TIMEOUT_LONG));
                executor.shutdownNow();

                // Verify layout contains only one node
                corfuClient.invalidateLayout();
                assertThat(corfuClient.getLayout().getAllServers()).containsExactly(server0.getParams().getEndpoint());

                // Verify data path working fine
                for (int x = 0; x < TestFixtureConst.DEFAULT_TABLE_ITER; x++) {
                    assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
                }
            });

            testCase.it("should concurrently add four nodes back into cluster", data -> {
                // Concurrently add four nodes back into cluster and wait for cluster to stabilize
                ExecutorService executor = Executors.newFixedThreadPool(numNodes - 1);
                servers.forEach(node -> executor.submit(() -> corfuClient.getManagementView().addNode(
                        node.getParams().getEndpoint(),
                        clientFixture.getNumRetry(),
                        clientFixture.getTimeout(),
                        clientFixture.getPollPeriod())
                ));

                Sleep.sleepUninterruptibly(Duration.ofSeconds(TestFixtureConst.DEFAULT_TIMEOUT_LONG));
                executor.shutdownNow();

                // Verify layout should contain all five nodes
                corfuClient.invalidateLayout();
                assertThat(corfuClient.getLayout().getAllServers().size()).isEqualTo(numNodes);

                // Verify data path working fine
                for (int x = 0; x < TestFixtureConst.DEFAULT_TABLE_ITER; x++) {
                    assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
                }
            });
        });
    }
}
