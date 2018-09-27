package org.corfudb.universe.node.vm;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.slf4j.event.Level;

import static org.corfudb.universe.node.CorfuServer.Mode;
import static org.corfudb.universe.node.CorfuServer.Persistence;
import static org.corfudb.universe.node.CorfuServer.ServerParams;


/**
 * Represents the parameters for constructing a {@link CorfuServerOnVm}.
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class VmServerParams extends ServerParams {
    private final String vmName;

    @Builder
    public VmServerParams(String vmName, String logDir, int port, Mode mode, Persistence persistence,
                          Level logLevel, String baseDir) {
        super(logDir, port, mode, persistence, logLevel, baseDir);
        this.vmName = vmName;
    }
}
