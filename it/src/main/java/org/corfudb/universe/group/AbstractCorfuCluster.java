package org.corfudb.universe.group;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.corfudb.universe.node.CorfuServer.ServerParams;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.util.ClassUtils;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static lombok.Builder.Default;

public abstract class AbstractCorfuCluster implements CorfuCluster {
    @Default
    protected final ConcurrentMap<String, Node> nodes = new ConcurrentHashMap<>();

    @Getter
    protected final CorfuClusterParams params;

    protected AbstractCorfuCluster(CorfuClusterParams params) {
        this.params = params;
    }

    protected abstract Node deployNode(ServerParams nodeParams);

    @Override
    public void stop(Duration timeout) {
        nodes.values().forEach(node -> node.stop(timeout));
    }

    @Override
    public void kill() {
        nodes.values().forEach(Node::kill);
    }

    @Override
    public Node add(Node.NodeParams nodeParams) {
        ServerParams serverParams = ClassUtils.cast(nodeParams);
        Node node = deployNode(serverParams);
        params.add(serverParams);
        return node;
    }

    @Override
    public <T extends Node> T getNode(String nodeName) {
        return ClassUtils.cast(nodes.get(nodeName));
    }

    @Override
    public <T extends Node> ImmutableMap<String, T> nodes() {
        return ClassUtils.cast(ImmutableMap.copyOf(nodes));
    }

}
