package org.corfudb.universe.group.vm;

import com.google.common.collect.ImmutableMap;
import com.vmware.vim25.mo.VirtualMachine;
import lombok.Builder;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.group.AbstractCorfuCluster;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.CorfuServer.ServerParams;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.Node.NodeParams;
import org.corfudb.universe.node.vm.CorfuServerOnVm;
import org.corfudb.universe.node.vm.VmServerParams;
import org.corfudb.universe.universe.vm.VmUniverse.VmUniverseParams;
import org.corfudb.universe.util.ClassUtils;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Provides VM implementation of {@link Group}.
 */
public class VmCorfuCluster extends AbstractCorfuCluster {
    private final VmUniverseParams universeParams;
    private final ImmutableMap<String, VirtualMachine> vms;
    private final ExecutorService executor;

    @Builder
    protected VmCorfuCluster(CorfuClusterParams params, VmUniverseParams universeParams,
                             ImmutableMap<String, VirtualMachine> vms, int numThreads) {
        super(params);
        this.universeParams = universeParams;
        this.vms = vms;
        this.executor = Executors.newFixedThreadPool(numThreads);
    }

    /**
     * Deploys a VM {@link Group}, including the following steps:
     * a) Deploy the Corfu nodes on VMs
     * b) Bootstrap all the nodes to form a cluster
     * @return an instance of {@link Group}
     */
    @Override
    public AbstractCorfuCluster deploy() {
        List<Node> deployment = params.getNodesParams()
                .stream()
                .map(this::deployNodeAsync)
                .collect(Collectors.toList())
                .stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

        deployment.forEach(node -> {
            VmServerParams params = getVmServerParams(node.getParams());
            nodes.put(params.getName(), node);
        });

        bootstrap();

        return this;
    }

    private CompletableFuture<Node> deployNodeAsync(ServerParams serverParams) {
        return CompletableFuture.supplyAsync(() -> deployNode(serverParams), executor);
    }

    /**
     * Deploys a Corfu server node according to the provided parameter.
     * @return an instance of {@link Node}
     */
    @Override
    protected Node deployNode(ServerParams serverParams) {
        VmServerParams params = getVmServerParams(serverParams);

        CorfuServer node = CorfuServerOnVm.builder()
                .universeParams(universeParams)
                .params(params)
                .vm(vms.get(params.getVmName()))
                .build();

        node.deploy();
        return node;
    }

    @Override
    public void bootstrap() {
        BootstrapUtil.bootstrap(getLayout(), params.getBootStrapRetries(), params.getRetryTimeout());
    }

    /**
     * @return an instance of {@link Layout} that is built from the existing parameters.
     */
    private Layout getLayout() {
        long epoch = 0;
        UUID clusterId = UUID.randomUUID();

        List<String> servers = params.getNodesParams()
                .stream()
                .map(params -> (VmServerParams) params)
                .map(vmParams -> vms.get(vmParams.getVmName()).getGuest().getIpAddress() + ":" + vmParams.getPort())
                .collect(Collectors.toList());

        Layout.LayoutSegment segment = new Layout.LayoutSegment(
                Layout.ReplicationMode.CHAIN_REPLICATION,
                0L,
                -1L,
                Collections.singletonList(new Layout.LayoutStripe(servers))
        );
        return new Layout(servers, servers, Collections.singletonList(segment), epoch, clusterId);
    }

    private VmServerParams getVmServerParams(NodeParams serverParams) {
        return ClassUtils.cast(serverParams, VmServerParams.class);
    }
}
