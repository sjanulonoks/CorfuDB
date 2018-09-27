package org.corfudb.universe.universe.vm;

import com.google.common.collect.ImmutableMap;
import com.vmware.vim25.CustomizationAdapterMapping;
import com.vmware.vim25.CustomizationDhcpIpGenerator;
import com.vmware.vim25.CustomizationFixedName;
import com.vmware.vim25.CustomizationGlobalIPSettings;
import com.vmware.vim25.CustomizationIPSettings;
import com.vmware.vim25.CustomizationLinuxOptions;
import com.vmware.vim25.CustomizationLinuxPrep;
import com.vmware.vim25.CustomizationSpec;
import com.vmware.vim25.GuestInfo;
import com.vmware.vim25.VirtualMachineCloneSpec;
import com.vmware.vim25.VirtualMachineRelocateSpec;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.Task;
import com.vmware.vim25.mo.VirtualMachine;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.group.vm.VmCorfuCluster;
import org.corfudb.universe.node.NodeException;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseException;
import org.corfudb.universe.util.ClassUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.RemoteException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * Represents VM implementation of a {@link Universe}.
 * <p>
 * The following are the main functionalities provided by this class:
 * </p>
 * DEPLOY: first deploys VMs on vSphere (if not exist), then deploys the group (corfu server) on the VMs
 * SHUTDOWN: stops the {@link Universe}, i.e. stops the existing {@link Group} gracefully within the provided timeout
 */
@Slf4j
public class VmUniverse implements Universe {
    @Default
    @Getter
    private final VmUniverseParams universeParams;

    private final ConcurrentMap<String, Group> groups = new ConcurrentHashMap<>();
    private final String universeId;
    private final int numThreads;
    private final ExecutorService executor = Executors.newFixedThreadPool(10);

    @Builder
    public VmUniverse(VmUniverseParams universeParams, int numThreads) {
        this.universeParams = universeParams;
        this.universeId = UUID.randomUUID().toString();
        this.numThreads = numThreads;
    }

    /**
     * Deploy a {@link Universe} according to provided parameter, vSphere APIs, and other components.
     *
     * @return Current instance of a VM {@link Universe} would be returned.
     * @throws UniverseException this exception will be thrown if deploying a {@link Universe} is not successful
     */
    @Override
    public VmUniverse deploy() {
        log.info("Deploy the universe: {}", universeId);
        Map<String, VirtualMachine> vms = new HashMap<>();

        universeParams.vmIpAddresses
                .keySet()
                .stream()
                .map(this::deployVmAsync)
                .map(CompletableFuture::join)
                .forEach(vm -> {
                    universeParams.updateIpAddress(vm.getName(), vm.getGuest().getIpAddress());
                    vms.put(vm.getName(), vm);
                });

        log.info("The deployed VMs are: {}", universeParams.vmIpAddresses);

        createAndDeployGroups(ImmutableMap.copyOf(vms));

        return this;
    }

    /**
     * Create and deploy VM {@link Group} one by one.
     */
    private void createAndDeployGroups(ImmutableMap<String, VirtualMachine> vms) {
        UniverseParams universeConfig = universeParams;
        for (String groupName : universeConfig.getGroups().keySet()) {
            deployVmGroup(universeConfig.getGroupParams(groupName, GroupParams.class), vms);
        }
    }

    /**
     * Deploy a {@link Group} on existing VMs according to input parameter.
     */
    private void deployVmGroup(GroupParams groupParams, ImmutableMap<String, VirtualMachine> vms) {
        switch (groupParams.getNodeType()) {
            case CORFU_SERVER:
                VmCorfuCluster cluster = VmCorfuCluster.builder()
                        .universeParams(universeParams)
                        .params(ClassUtils.cast(groupParams))
                        .vms(vms)
                        .numThreads(numThreads)
                        .build();

                cluster.deploy();

                groups.put(groupParams.getName(), cluster);
                break;
            case CORFU_CLIENT:
                throw new UniverseException("Not implemented corfu client. Group config: " + groupParams);
            default:
                throw new UniverseException("Unknown node type");
        }
    }

    /**
     * Shutdown the {@link Universe} by stopping each of its {@link Group}.
     */
    @Override
    public void shutdown() {
        log.info("Shutdown the universe: {}", universeId);

        groups.values().forEach(group -> {
            try {
                group.stop(universeParams.getTimeout());
            } catch (Exception ex) {
                log.info("Can't stop group: {}", group.getParams(), ex);
            }
        });
    }

    @Override
    public Universe add(GroupParams groupParams) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ImmutableMap<String, Group> groups() {
        return ImmutableMap.copyOf(groups);
    }

    @Override
    public <T extends Group> T getGroup(String groupName) {
        return ClassUtils.cast(groups.get(groupName));
    }

    private CompletableFuture<VirtualMachine> deployVmAsync(String vmName) throws NodeException {
        log.info("Deploy vm asynchronously: {}", vmName);
        return CompletableFuture.supplyAsync(() -> deployVm(vmName), executor);
    }

    /**
     * Deploy and power on a VM appliance in vSphere.
     *
     * @return VirtualMachine instance
     */
    private VirtualMachine deployVm(String vmName) throws NodeException {
        log.info("Deploy VM: {}", vmName);

        VirtualMachine vm;
        try {
            //Connect to vSphere server using VIJAVA
            VmUniverseParams params = universeParams;
            ServiceInstance si = new ServiceInstance(
                    new URL(params.getVSphereUrl()),
                    params.getVSphereUsername(),
                    params.getVSpherePassword(),
                    true
            );

            InventoryNavigator inventoryNavigator = new InventoryNavigator(si.getRootFolder());

            //First check if a VM with this name already exists or not
            vm = (VirtualMachine) inventoryNavigator.searchManagedEntity("VirtualMachine", vmName);
            if (vm != null) {
                return vm;
            }

            //Find the template machine in the inventory
            VirtualMachine vmTemplate = (VirtualMachine) inventoryNavigator.searchManagedEntity(
                    ManagedEntityType.VIRTUAL_MACHINE.typeName,
                    params.getTemplateVMName()
            );

            log.info("Deploying the VM {} via vSphere {}...", vmName, params.getVSphereUrl());

            // Create customization for cloning process
            VirtualMachineCloneSpec cloneSpec = createLinuxCustomization(vmName);
            try {
                //Do the cloning - providing the clone specification
                Task cloneTask = vmTemplate.cloneVM_Task((Folder) vmTemplate.getParent(), vmName, cloneSpec);
                cloneTask.waitForTask();
            } catch (RemoteException | InterruptedException e) {
                throw new UniverseException(String.format("Deploy VM %s failed due to ", vmName), e);
            }
            // After the clone task completes, get the VM from the inventory
            vm = (VirtualMachine) inventoryNavigator.searchManagedEntity("VirtualMachine", vmName);

            String cloneVmIpAddress = null;
            log.info("Getting IP address for {} from DHCP...please wait...", vmName);
            while (cloneVmIpAddress == null) {
                TimeUnit.SECONDS.sleep(5);
                GuestInfo guest = vm.getGuest();
                cloneVmIpAddress = guest.getIpAddress();
            }
        } catch (RemoteException | MalformedURLException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UniverseException(String.format("Deploy VM %s failed due to ", vmName), e);
        }
        return vm;
    }

    /**
     * Create the Linux customization for a specific VM to be cloned.
     *
     * @return VirtualMachineCloneSpec instance
     */
    public static VirtualMachineCloneSpec createLinuxCustomization(String cloneName) {
        VirtualMachineCloneSpec vmCloneSpec = new VirtualMachineCloneSpec();

        //Set location of clone to be the same as template (Datastore)
        VirtualMachineRelocateSpec vmRelocateSpec = new VirtualMachineRelocateSpec();
        vmCloneSpec.setLocation(vmRelocateSpec);

        //Clone is powered on, not a template.
        vmCloneSpec.setPowerOn(true);
        vmCloneSpec.setTemplate(false);

        //Create customization specs/linux specific options
        CustomizationSpec customSpec = new CustomizationSpec();
        CustomizationLinuxOptions linuxOptions = new CustomizationLinuxOptions();
        customSpec.setOptions(linuxOptions);

        CustomizationLinuxPrep linuxPrep = new CustomizationLinuxPrep();
        linuxPrep.setDomain("eng.vmware.com");
        linuxPrep.setHwClockUTC(true);
        linuxPrep.setTimeZone("America/Los_Angeles");

        CustomizationFixedName fixedName = new CustomizationFixedName();
        fixedName.setName(cloneName);
        linuxPrep.setHostName(fixedName);
        customSpec.setIdentity(linuxPrep);

        //Network related settings
        CustomizationGlobalIPSettings globalIPSettings = new CustomizationGlobalIPSettings();
        globalIPSettings.setDnsServerList(new String[]{"10.172.40.1", "10.172.40.2"});
        globalIPSettings.setDnsSuffixList(new String[]{"eng.vmware.com", "vmware.com"});
        customSpec.setGlobalIPSettings(globalIPSettings);

        CustomizationIPSettings customizationIPSettings = new CustomizationIPSettings();
        customizationIPSettings.setIp(new CustomizationDhcpIpGenerator());
        customizationIPSettings.setGateway(new String[]{"10.172.211.253"});
        customizationIPSettings.setSubnetMask("255.255.255.0");

        CustomizationAdapterMapping adapterMapping = new CustomizationAdapterMapping();
        adapterMapping.setAdapter(customizationIPSettings);

        CustomizationAdapterMapping[] adapterMappings = new CustomizationAdapterMapping[]{adapterMapping};
        customSpec.setNicSettingMap(adapterMappings);

        //Set all customization to clone specs
        vmCloneSpec.setCustomization(customSpec);
        return vmCloneSpec;
    }

    /**
     * Represents the parameters for constructing a VM {@link Universe}.
     */
    @Getter
    public static class VmUniverseParams extends UniverseParams {
        private final String vSphereUrl;
        private final String vSphereUsername;
        private final String vSpherePassword;
        private final String templateVMName;
        private final String vmUserName;
        private final String vmPassword;
        private final ConcurrentMap<String, String> vmIpAddresses;

        @Builder
        public VmUniverseParams(String vSphereUrl, String vSphereUsername, String vSpherePassword,
                                String templateVMName, String vmUserName, String vmPassword,
                                ConcurrentMap<String, String> vmIpAddresses, String networkName,
                                ConcurrentMap<String, GroupParams> groups, Duration timeout) {
            super(networkName, groups, timeout);
            this.vSphereUrl = vSphereUrl;
            this.vSphereUsername = vSphereUsername;

            this.vSpherePassword = vSpherePassword;
            this.templateVMName = templateVMName;
            this.vmUserName = vmUserName;
            this.vmPassword = vmPassword;
            this.vmIpAddresses = vmIpAddresses;
        }


        public VmUniverseParams updateIpAddress(String vmName, String ipAddress) {
            vmIpAddresses.put(vmName, ipAddress);
            return this;
        }
    }

    enum ManagedEntityType {
        VIRTUAL_MACHINE("VirtualMachine");

        private final String typeName;

        ManagedEntityType(String typeName) {
            this.typeName = typeName;
        }
    }
}
