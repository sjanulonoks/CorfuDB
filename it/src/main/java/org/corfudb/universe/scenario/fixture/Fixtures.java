package org.corfudb.universe.scenario.fixture;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Getter;
import org.corfudb.universe.group.CorfuCluster.CorfuClusterParams;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.node.CorfuClient.ClientParams;
import org.corfudb.universe.node.Node;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static lombok.Builder.Default;
import static org.corfudb.universe.node.CorfuServer.*;
import static org.corfudb.universe.universe.Universe.UniverseParams;

/**
 * Fixture factory provides predefined fixtures
 */
public interface Fixtures {

    class TestFixtureConst {

        public static final int DEFAULT_TIMEOUT = 30;

        public static final int DEFAULT_TIMEOUT_MEDIUM = 60;

        public static final int DEFAULT_TIMEOUT_LONG = 100;

        public static final String DEFAULT_STREAM_NAME = "stream";

        public static final int DEFAULT_TABLE_ITER = 100;
    }

    @Builder
    @Getter
    class CorfuClientFixture implements Fixture<ClientParams> {
        @Default
        private final int numRetry = 5;
        @Default
        private final Duration timeout = Duration.ofSeconds(30);
        @Default
        private final Duration pollPeriod = Duration.ofMillis(50);

        @Override
        public ClientParams data() {
            return ClientParams.builder()
                    .numRetry(numRetry)
                    .timeout(timeout)
                    .pollPeriod(pollPeriod)
                    .build();
        }
    }

    @Builder
    @Getter
    class SingleServerFixture implements Fixture<ServerParams> {
        @Default
        private final int port = 9000;
        @Default
        private final Mode mode = Mode.CLUSTER;

        @Override
        public ServerParams data() {
            return ServerParams.builder()
                    .mode(mode)
                    .streamLogDir("/tmp/")
                    .logLevel(Level.TRACE)
                    .persistence(Persistence.DISK)
                    .port(port)
                    .build();
        }
    }

    @Builder
    @Getter
    class MultipleServersFixture implements Fixture<ImmutableList<ServerParams>> {
        @Default
        private final int numNodes = 3;

        @Override
        public ImmutableList<ServerParams> data() {
            List<ServerParams> serversParams = new ArrayList<>();

            for (int i = 0; i < numNodes; i++) {
                final int port = 9000 + i;
                Mode mode = Mode.CLUSTER;

                ServerParams serverParam = SingleServerFixture
                        .builder()
                        .port(port)
                        .mode(mode)
                        .build()
                        .data();

                serversParams.add(serverParam);
            }
            return ImmutableList.copyOf(serversParams);
        }
    }

    @Builder
    @Getter
    class CorfuGroupFixture implements Fixture<GroupParams> {
        @Default
        private final MultipleServersFixture servers = MultipleServersFixture.builder().build();
        @Default
        private final CorfuClientFixture client = CorfuClientFixture.builder().build();
        @Default
        private final String groupName = "corfuCluster";

        @Override
        public GroupParams data() {
            CorfuClusterParams params = CorfuClusterParams.builder()
                    .name(groupName)
                    .nodeType(Node.NodeType.CORFU_SERVER)
                    .build();

            servers.data().forEach(params::add);

            return params;
        }
    }

    @Builder
    @Getter
    class UniverseFixture implements Fixture<UniverseParams> {
        @Default
        private final CorfuGroupFixture group = CorfuGroupFixture.builder().build();

        @Override
        public UniverseParams data() {
            GroupParams groupParams = group.data();
            return UniverseParams.builder()
                    .build()
                    .add(groupParams);
        }
    }
}
