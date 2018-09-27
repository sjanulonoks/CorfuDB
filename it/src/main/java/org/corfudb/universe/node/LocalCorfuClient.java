package org.corfudb.universe.node;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.ManagementView;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.universe.node.CorfuServer.ServerParams;
import org.corfudb.util.NodeLocator;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static org.corfudb.runtime.CorfuRuntime.fromParameters;

@Slf4j
public class LocalCorfuClient implements CorfuClient {
    private final CorfuRuntime runtime;
    @Getter
    private final ClientParams params;
    @Getter
    private final ImmutableList<String> serverEndpoints;

    @Builder
    public LocalCorfuClient(ClientParams params, ImmutableList<String> serverEndpoints) {
        this.params = params;
        this.serverEndpoints = serverEndpoints;

        List<NodeLocator> layoutServers = serverEndpoints.stream()
                .map(NodeLocator::parseString)
                .collect(Collectors.toList());

        CorfuRuntime.CorfuRuntimeParameters runtimeParams = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .layoutServers(layoutServers)
                .build();

        this.runtime = fromParameters(runtimeParams);
    }

    @Override
    public LocalCorfuClient deploy() {
        connect();
        return this;
    }

    public boolean add(ServerParams serverParams) {
        log.debug("Add node: {}", serverParams);

        runtime.getManagementView().addNode(
                serverParams.getEndpoint(),
                params.getNumRetry(),
                params.getTimeout(),
                params.getPollPeriod()
        );

        return true;
    }

    public boolean remove(ServerParams serverParams) {
        log.debug("Remove node: {}", serverParams);

        runtime.getManagementView().removeNode(
                serverParams.getEndpoint(),
                params.getNumRetry(),
                params.getTimeout(),
                params.getPollPeriod()
        );

        return true;
    }

    @Override
    public void stop(Duration timeout) {
        runtime.shutdown();
    }

    @Override
    public void kill() {
        runtime.shutdown();
    }

    @Override
    public void destroy() {
        runtime.shutdown();
    }

    public CorfuTable createDefaultCorfuTable(String streamName) {
        return runtime.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(streamName)
                .open();
    }

    public Layout getLayout() {
        return runtime.getLayoutView().getLayout();
    }

    public ObjectsView getObjectsView() {
        return runtime.getObjectsView();
    }

    private void connect() {
        runtime.connect();
    }

    public ManagementView getManagementView() {
        return runtime.getManagementView();
    }

    public void invalidateLayout() {
        runtime.invalidateLayout();
    }
}
