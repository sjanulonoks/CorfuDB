package org.corfudb.universe.universe.vm;

import org.corfudb.universe.UniverseFactory;
import org.corfudb.universe.group.CorfuCluster.CorfuClusterParams;
import org.corfudb.universe.node.vm.VmServerParams;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.vm.VmUniverse.VmUniverseParams;
import org.junit.Test;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.corfudb.universe.group.Group.GroupParams;
import static org.corfudb.universe.node.CorfuServer.*;

public class VmUniverseIT {

    /**
     * Conducts a basic test that deploys a VM {@link Universe} then shutdown.
     * @throws Exception an error
     */
    @Test
    public void deployVmsAndShutdownTest() throws Exception {
        ConcurrentMap<String, String> vmIpAddresses = new ConcurrentHashMap<>();
        vmIpAddresses.put("corfu-vm-1", "0.0.0.0");
        vmIpAddresses.put("corfu-vm-2", "0.0.0.0");
        vmIpAddresses.put("corfu-vm-3", "0.0.0.0");

        final int port = 9000;
        final Mode mode = Mode.CLUSTER;

        VmServerParams server1 = VmServerParams.builder()
                .mode(mode)
                .logDir("/tmp/")
                .logLevel(Level.TRACE)
                .persistence(Persistence.DISK)
                .port(port)
                .timeout(Duration.ofMinutes(5))
                .pollPeriod(Duration.ofMillis(50))
                .workflowNumRetry(3)
                .vmName("corfu-vm-1")
                .build();

        VmServerParams server2 = VmServerParams.builder()
                .mode(mode)
                .logDir("/tmp/")
                .logLevel(Level.TRACE)
                .persistence(Persistence.DISK)
                .port(port)
                .timeout(Duration.ofMinutes(5))
                .pollPeriod(Duration.ofMillis(50))
                .workflowNumRetry(3)
                .vmName("corfu-vm-2")
                .build();

        VmServerParams server3 = VmServerParams.builder()
                .mode(mode)
                .logDir("/tmp/")
                .logLevel(Level.TRACE)
                .persistence(Persistence.DISK)
                .port(port)
                .timeout(Duration.ofMinutes(5))
                .pollPeriod(Duration.ofMillis(50))
                .workflowNumRetry(3)
                .vmName("corfu-vm-3")
                .build();

        GroupParams corfuCluster = CorfuClusterParams.builder()
                .name("corfuCluster")
                .bootStrapRetries(3)
                .nodeType(NodeType.CORFU_SERVER)
                .nodes(Arrays.asList(server1, server2, server3))
                .build();

        ConcurrentMap<String, GroupParams> groups = new ConcurrentHashMap<>();
        groups.put("corfuCluster", corfuCluster);

        VmUniverseParams params = VmUniverseParams.builder()
                .vmIpAddresses(vmIpAddresses)
                .vSphereUrl("https://10.173.65.98/sdk")
                .vSphereUsername("administrator@vsphere.local")
                .vSpherePassword("VMware1VMware!")
                .templateVMName("IntegTestVMTemplate")
                .vmUserName("vmware")
                .vmPassword("vmware")
                .groups(groups)
                .build();


        VmUniverse vmUniverse = UniverseFactory.getInstance().buildVmUniverse(params);
        vmUniverse.deploy();
        vmUniverse.shutdown();
    }
}
