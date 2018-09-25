package org.corfudb.universe.node.vm;

import com.vmware.vim25.GuestInfo;
import com.vmware.vim25.mo.VirtualMachine;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.group.vm.RemoteOperationHelper;
import org.corfudb.universe.node.AbstractCorfuServer;
import org.corfudb.universe.node.CorfuServer;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.corfudb.universe.universe.vm.VmUniverse.VmUniverseParams;

/**
 * Implements a {@link CorfuServer} instance that is running on VM.
 */
@Slf4j
public class CorfuServerOnVm extends AbstractCorfuServer<VmServerParams> {

    private final VirtualMachine vm;
    private final VmUniverseParams universeParams;
    private final String ipAddress;

    @Builder
    public CorfuServerOnVm(VmServerParams params, VirtualMachine vm, VmUniverseParams universeParams) {
        super(params);
        this.vm = vm;
        this.universeParams = universeParams;
        this.ipAddress = getIpAddress();
    }

    /**
     * Deploys a Corfu server on the VM as specified, including the following steps:
     * a) Copy the corfu jar file under the working directory to the VM
     * b) Run that jar file using java on the VM
     */
    @Override
    public CorfuServer deploy() {
        executeOnVm("mkdir -p ./" + params.getName());

        RemoteOperationHelper.scpToVm(
                ipAddress,
                universeParams.getVmUserName(),
                universeParams.getVmPassword(),
                "./target/corfu/infrastructure-0.2.2-SNAPSHOT-shaded.jar",
                "./" + params.getName() + "/corfu-server.jar"
        );

        // Compose command line for starting Corfu
        StringBuilder cmdLine = new StringBuilder();
        cmdLine.append("sh -c 'nohup java -cp ./" + params.getName() + "/*.jar org.corfudb.infrastructure.CorfuServer ");
        cmdLine.append(getCommandLineParams());
        cmdLine.append(" > /dev/null 2>&1 &'");

        executeOnVm(cmdLine.toString());

        return this;
    }

    /**
     * Executes a certain command on the VM.
     */
    private void executeOnVm(String cmdLine) {
        String ipAddress = getIpAddress();

        RemoteOperationHelper.execCommandOnVm(ipAddress,
                universeParams.getVmUserName(),
                universeParams.getVmPassword(),
                cmdLine
        );
    }

    /**
     * @return the IpAddress of this VM.
     */
    private String getIpAddress() {
        GuestInfo guest = vm.getGuest();
        return guest.getIpAddress();
    }

    /**
     * @param timeout a limit within which the method attempts to gracefully stop the {@link CorfuServer}.
     */
    @Override
    public void stop(Duration timeout) {
        try {
            CompletableFuture
                    .supplyAsync(() -> {
                        executeOnVm("pkill -f corfudb");
                        return true;
                    })
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Can't stop the corfu server. Params: {}", params);
            Thread.currentThread().interrupt();
            kill();
        } catch (ExecutionException | TimeoutException e) {
            kill();
        }
    }

    /**
     * Kill the Corfu server process on the VM directly.
     */
    @Override
    public void kill() {
        log.debug("Kill the corfu server. Params: {}", params);
        executeOnVm("pkill -f -9 corfudb");
    }
}
