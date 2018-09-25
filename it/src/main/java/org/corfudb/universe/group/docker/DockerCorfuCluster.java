package org.corfudb.universe.group.docker;

import com.spotify.docker.client.DockerClient;
import lombok.Builder;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.group.AbstractCorfuCluster;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.docker.CorfuServerDockerized;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.corfudb.universe.node.CorfuServer.ServerParams;
import static org.corfudb.universe.universe.Universe.UniverseParams;

/**
 * Provides Docker implementation of {@link Group}.
 */
public class DockerCorfuCluster extends AbstractCorfuCluster {

    private final DockerClient docker;
    private final UniverseParams universeParams;

    @Builder
    public DockerCorfuCluster(CorfuClusterParams params, DockerClient docker, UniverseParams universeParams) {
        super(params);
        this.docker = docker;
        this.universeParams = universeParams;
    }

    @Override
    public AbstractCorfuCluster deploy() {
        for (ServerParams nodeParams : params.getNodesParams()) {
            Node node = deployNode(nodeParams);
            nodes.put(node.getParams().getName(), node);
        }

        bootstrap();

        return this;
    }

    @Override
    protected Node deployNode(ServerParams nodeParams) {
        CorfuServer node = CorfuServerDockerized.builder()
                .universeParams(universeParams)
                .params(nodeParams)
                .docker(docker)
                .build();

        node.deploy();
        return node;
    }

    @Override
    public void bootstrap() {
        BootstrapUtil.bootstrap(getLayout(), params.getBootStrapRetries(), params.getRetryTimeout());
    }

    private Layout getLayout() {
        long epoch = 0;
        UUID clusterId = UUID.randomUUID();
        List<String> servers = params.getServers();

        Layout.LayoutSegment segment = new Layout.LayoutSegment(
                Layout.ReplicationMode.CHAIN_REPLICATION,
                0L,
                -1L,
                Collections.singletonList(new Layout.LayoutStripe(params.getServers()))
        );
        return new Layout(servers, servers, Collections.singletonList(segment), epoch, clusterId);
    }
}
