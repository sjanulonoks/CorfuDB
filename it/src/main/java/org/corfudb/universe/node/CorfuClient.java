package org.corfudb.universe.node;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Exclude;
import lombok.Getter;

import java.time.Duration;

/**
 * Represent a Corfu client implementation of {@link Node}.
 */
public interface CorfuClient extends Node {
    ClientParams getParams();

    @Builder
    @Getter
    @EqualsAndHashCode
    class ClientParams implements NodeParams {
        private final String host;
        private final String name;
        private final NodeType nodeType = NodeType.CORFU_CLIENT;

        @Exclude
        private final int numRetry;
        @Exclude
        private final Duration timeout;
        @Exclude
        private final Duration pollPeriod;
    }
}
