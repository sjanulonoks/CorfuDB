package org.corfudb.universe.node;

import lombok.*;
import org.corfudb.universe.universe.Universe;
import org.slf4j.event.Level;

import static lombok.EqualsAndHashCode.Exclude;

/**
 * Represent a Corfu server implementation of {@link Node} used in the {@link Universe}.
 */
public interface CorfuServer extends Node {

    @Override
    CorfuServer deploy();

    ServerParams getParams();

    /**
     * Disconnect a CorfuServer from the network
     *
     * @throws NodeException thrown in case of unsuccessful disconnect.
     */
    void disconnect();

    /**
     * Pause a CorfuServer
     *
     * @throws NodeException thrown in case of unsuccessful resume.
     */
    void pause();

    /**
     * Restart a {@link CorfuServer}
     *
     * @throws NodeException this exception will be thrown if the node can not be restarted
     */
    void restart();

    /**
     * Reconnect a {@link CorfuServer} to the network
     *
     * @throws NodeException this exception will be thrown if the node can not be reconnected
     */
    void reconnect();

    /**
     * Resume a {@link CorfuServer}
     *
     * @throws NodeException this exception will be thrown if the node can not be unpaused
     */
    void resume();

    enum Mode {
        SINGLE, CLUSTER
    }

    enum Persistence {
        DISK, MEMORY
    }

    @Builder
    @Getter
    @AllArgsConstructor
    @EqualsAndHashCode
    @ToString
    class ServerParams implements NodeParams {
        @Exclude
        private final String streamLogDir;
        private final int port;
        private final Mode mode;
        private final Persistence persistence;
        @Exclude
        private final Level logLevel;
        private final NodeType nodeType = NodeType.CORFU_SERVER;

        public String getName() {
            return "node" + port;
        }

        public String getEndpoint() {
            return getName() + ":" + port;
        }
    }
}
