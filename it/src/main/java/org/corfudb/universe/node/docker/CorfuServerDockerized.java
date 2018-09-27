package org.corfudb.universe.node.docker;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerClient.ExecCreateParam;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.ExecCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.IpamConfig;
import com.spotify.docker.client.messages.PortBinding;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.node.AbstractCorfuServer;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.CorfuServer.ServerParams;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.NodeException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.spotify.docker.client.DockerClient.LogsParam;
import static org.corfudb.universe.universe.Universe.UniverseParams;

/**
 * Implements a docker instance representing a {@link CorfuServer}.
 */
@Slf4j
public class CorfuServerDockerized extends AbstractCorfuServer<ServerParams> {
    private static final String IMAGE_NAME = "corfu-server:" + getAppVersion();
    public static final String ALL_NETWORK_INTERFACES = "0.0.0.0";

    private final DockerClient docker;

    @Builder
    public CorfuServerDockerized(DockerClient docker, ServerParams params, UniverseParams universeParams) {
        super(params, universeParams);
        this.docker = docker;
    }

    /**
     * Deploys a Corfu server / docker container
     */
    @Override
    public CorfuServerDockerized deploy() {
        log.info("Deploying the Corfu server. Docker container: {}", params.getName());

        deployContainer();

        return this;
    }

    /**
     * This method attempts to gracefully stop the Corfu server. After timeout, it will kill the Corfu server.
     *
     * @param timeout a duration after which the stop will kill the server
     * @throws NodeException this exception will be thrown if the server cannot be stopped.
     */
    @Override
    public void stop(Duration timeout) {
        log.info("Stopping the Corfu server. Docker container: {}", params.getName());

        try {
            ContainerInfo container = docker.inspectContainer(params.getName());
            if (!container.state().running() && !container.state().paused()) {
                log.warn("The container `{}` is already stopped", container.name());
                return;
            }
            docker.stopContainer(params.getName(), (int) timeout.getSeconds());
        } catch (DockerException | InterruptedException e) {
            throw new NodeException("Can't stop Corfu server", e);
        }
    }

    /**
     * Immediately kill the Corfu server.
     *
     * @throws NodeException this exception will be thrown if the server can not be killed.
     */
    @Override
    public void kill() {
        log.info("Killing the Corfu server. Docker container: {}", params.getName());

        try {
            ContainerInfo container = docker.inspectContainer(params.getName());
            if (!container.state().running() && !container.state().paused()) {
                log.warn("The container `{}` is not running", container.name());
                return;
            }
            docker.killContainer(params.getName());
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't kill Corfu server: " + params.getName());
        }
    }

    /**
     * Immediately kill and  remove the docker container
     *
     * @throws NodeException this exception will be thrown if the server can not be killed.
     */
    @Override
    public void destroy() {
        log.info("Destroying the Corfu server. Docker container: {}", params.getName());

        try {
            kill();
        } catch (NodeException ex) {
            log.warn("Can't kill container: {}", params.getName());
        }

        collectLogs();

        try {
            docker.removeContainer(params.getName());
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't destroy Corfu server: " + params.getName());
        }
    }

    /**
     * Disconnect the container from docker network
     *
     * @throws NodeException this exception will be thrown if the server can not be disconnected
     */
    @Override
    public void disconnect() {
        log.info("Disconnecting the server from docker network. Docker container: {}", params.getName());

        try {
            String networkName = universeParams.getNetworkName();
            IpamConfig ipamConfig = docker.inspectNetwork(networkName).ipam().config().get(0);
            String subnet = ipamConfig.subnet();
            String gateway = ipamConfig.gateway();

            // iptables -A INPUT -s $gateway -j ACCEPT
            execCommand("iptables", "-A", "INPUT", "-s", gateway, "-j", "ACCEPT");
            // iptables -A INPUT -s $subnet -j DROP
            execCommand("iptables", "-A", "INPUT", "-s", subnet, "-j", "DROP");
            // iptables -A OUTPUT -s $gateway -j ACCEPT
            execCommand("iptables", "-A", "OUTPUT", "-d", gateway, "-j", "ACCEPT");
            // iptables -A OUTPUT -s $subnet -j DROP
            execCommand("iptables", "-A", "OUTPUT", "-d", subnet, "-j", "DROP");
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't disconnect container from docker network " + params.getName());
        }
    }

    /**
     * Pause the container from docker network
     *
     * @throws NodeException this exception will be thrown if the server can not be paused
     */
    @Override
    public void pause() {
        log.info("Pausing the Corfu server: {}", params.getName());

        try {
            ContainerInfo container = docker.inspectContainer(params.getName());
            if (!container.state().running()) {
                log.warn("The container `{}` is not running", container.name());
                return;
            }
            docker.pauseContainer(params.getName());
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't pause container " + params.getName());
        }
    }

    /**
     * Restart a {@link Node}
     *
     * @throws NodeException this exception will be thrown if the server can not be restarted
     */
    @Override
    public void restart() {
        log.info("Restarting the corfu server: {}", params.getName());

        try {
            ContainerInfo container = docker.inspectContainer(params.getName());
            if (container.state().running() || container.state().paused()) {
                log.warn("The container `{}` already running, should stop before restart", container.name());
                return;
            }
            docker.restartContainer(params.getName());
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't restart container " + params.getName(), ex);
        }
    }

    /**
     * Reconnect a {@link Node} to the network
     *
     * @throws NodeException this exception will be thrown if the node can not be reconnected
     */
    @Override
    public void reconnect() {
        log.info("Reconnecting the corfu server to the network. Docker container: {}", params.getName());

        try {
            execCommand("iptables", "-F", "INPUT");
            execCommand("iptables", "-F", "OUTPUT");
        } catch (DockerException | InterruptedException e) {
            throw new NodeException("Can't reconnect container to docker network " + params.getName());
        }
    }

    /**
     * Resume a {@link CorfuServer}
     *
     * @throws NodeException this exception will be thrown if the node can not be resumed
     */
    @Override
    public void resume() {
        log.info("Resuming the corfu server: {}", params.getName());

        try {
            ContainerInfo container = docker.inspectContainer(params.getName());
            if (!container.state().paused()) {
                log.warn("The container `{}` is not paused, should pause before resuming", container.name());
                return;
            }
            docker.unpauseContainer(params.getName());
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't resume container " + params.getName(), ex);
        }
    }

    /**
     * Deploy and start docker container, expose ports, connect to a network
     *
     * @return docker container id
     */
    private String deployContainer() {
        ContainerConfig containerConfig = buildContainerConfig();

        String id;
        try {
            ContainerCreation container = docker.createContainer(containerConfig, params.getName());
            id = container.id();

            docker.disconnectFromNetwork(id, "bridge");
            docker.connectToNetwork(id, docker.inspectNetwork(universeParams.getNetworkName()).id());

            docker.startContainer(id);
        } catch (InterruptedException | DockerException e) {
            throw new NodeException("Can't start a container", e);
        }

        return id;
    }

    private ContainerConfig buildContainerConfig() {
        // Bind ports
        String[] ports = {String.valueOf(params.getPort())};
        Map<String, List<PortBinding>> portBindings = new HashMap<>();
        for (String port : ports) {
            List<PortBinding> hostPorts = new ArrayList<>();
            hostPorts.add(PortBinding.of(ALL_NETWORK_INTERFACES, port));
            portBindings.put(port, hostPorts);
        }

        HostConfig hostConfig = HostConfig.builder()
                .privileged(true)
                .portBindings(portBindings)
                .build();

        // Compose command line for starting Corfu
        String cmdLine = String.format(
                "java -cp *.jar org.corfudb.infrastructure.CorfuServer %s",
                getCommandLineParams()
        );

        return ContainerConfig.builder()
                .hostConfig(hostConfig)
                .image(IMAGE_NAME)
                .hostname(params.getName())
                .exposedPorts(ports)
                .cmd("sh", "-c", cmdLine)
                .build();
    }

    /**
     * Run `docker exec` on a container
     */
    private void execCommand(String... command) throws DockerException, InterruptedException {
        log.info("Executing docker command: {}", String.join(" ", command));

        ExecCreation execCreation = docker.execCreate(
                params.getName(),
                command,
                ExecCreateParam.attachStdout(),
                ExecCreateParam.attachStderr()
        );

        log.info("docker exec result: {}", docker.execStart(execCreation.id()).readFully());
    }

    /**
     * Collect logs from container and write to the log directory
     */
    private void collectLogs() {
        log.debug("Collect logs for: {}", params.getName());

        try (LogStream stream = docker.logs(params.getName(), LogsParam.stdout(), LogsParam.stderr())) {
            String logs = stream.readFully();

            Path filePathObj = params.getServerLogDir().resolve(params.getName() + ".log");
            Files.write(filePathObj, logs.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (InterruptedException | DockerException | IOException e) {
            log.error("Can't collect logs from container: {}", params.getName());
        }
    }
}
