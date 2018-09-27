package org.corfudb.universe.node.vm;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.slf4j.event.Level;

import java.time.Duration;

import static org.corfudb.universe.node.CorfuServer.*;


/**
 * Represents the parameters for constructing a {@link CorfuServerOnVm}.
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class VmServerParams extends ServerParams {
    private final String vmName;

    @Builder
    public VmServerParams(String vmName, String logDir, int port, Mode mode,  Persistence persistence,
                          Level logLevel, int workflowNumRetry, Duration timeout, Duration pollPeriod) {
        super(logDir, port, mode, persistence, logLevel, workflowNumRetry, timeout, pollPeriod);
        this.vmName = vmName;
    }
}
