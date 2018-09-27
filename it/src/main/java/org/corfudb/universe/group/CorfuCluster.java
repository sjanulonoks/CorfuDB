package org.corfudb.universe.group;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.corfudb.universe.node.LocalCorfuClient;
import org.corfudb.universe.node.Node.NodeType;
import org.corfudb.universe.util.ClassUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.corfudb.universe.node.CorfuServer.ServerParams;

public interface CorfuCluster extends Cluster {

    LocalCorfuClient getLocalCorfuClient(ImmutableList<String> layoutServers);

    @Builder
    @EqualsAndHashCode
    class CorfuClusterParams implements GroupParams {
        @Getter
        private String name;
        @Default
        private final List<ServerParams> nodes = new ArrayList<>();
        @Getter
        private NodeType nodeType;
        @Default
        @Getter
        private final int bootStrapRetries = 3;
        @Default
        @Getter
        private final Duration retryTimeout = Duration.ofSeconds(10);

        @Override
        public ImmutableList<ServerParams> getNodesParams() {
            return ImmutableList.copyOf(nodes);
        }

        public ImmutableList<String> getServers() {
            List<String> servers = nodes.stream()
                    .map(ServerParams::getEndpoint)
                    .collect(Collectors.toList());

            return ImmutableList.copyOf(servers);
        }

        public synchronized CorfuClusterParams add(ServerParams nodeParams) {
            nodes.add(ClassUtils.cast(nodeParams));
            return this;
        }
    }
}
