package org.corfudb.universe.node;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.universe.universe.Universe;
import org.slf4j.event.Level;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

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

    @Builder(builderMethodName = "serverParamsBuilder")
    @Getter
    @AllArgsConstructor
    @EqualsAndHashCode
    @ToString
    class ServerParams implements NodeParams {
        private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss");
        private static final String CORFU_DB_PATH = "corfudb";
        private static final String BASE_DIR = "/tmp/";

        @Exclude
        private final String streamLogDir;
        private final int port;
        private final Mode mode;
        private final Persistence persistence;
        @Exclude
        private final Level logLevel;
        private final NodeType nodeType = NodeType.CORFU_SERVER;

        @Getter
        @Default
        private final String baseDir = BASE_DIR;

        public String getName() {
            return "node" + port;
        }

        public String getEndpoint() {
            return getName() + ":" + port;
        }

        public Path getServerLogDir() {
            String clusterLogPath = getName() + "_" + LocalDateTime.now().format(DATE_FORMATTER);
            return Paths.get(baseDir, CORFU_DB_PATH, clusterLogPath);
        }
    }
}
