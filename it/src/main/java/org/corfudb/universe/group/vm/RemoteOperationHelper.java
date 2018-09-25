package org.corfudb.universe.group.vm;

import lombok.extern.slf4j.Slf4j;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.optional.ssh.SSHExec;
import org.apache.tools.ant.taskdefs.optional.ssh.Scp;


/**
 * Provides the helper functions that do operations (copy file/execute command) on a remote machine.
 */
@Slf4j
public class RemoteOperationHelper {
    private static final Project PROJECT = new Project();

    public static void scpToVm(String vmIpAddress, String userName, String password, String localFile, String remoteDir) {
        Scp scp = new Scp();

        scp.setLocalFile(localFile);
        scp.setTodir(userName + ":" + password + "@" + vmIpAddress + ":" + remoteDir);
        scp.setProject(PROJECT);
        scp.setTrust(true);
        log.info("Copying " + localFile + " to " + remoteDir + " on " + vmIpAddress);
        scp.execute();
    }

    public static void execCommandOnVm(String vmIpAddress, String userName, String password, String command) {
        SSHExec sshExec = new SSHExec();

        sshExec.setUsername(userName);
        sshExec.setPassword(password);
        sshExec.setHost(vmIpAddress);
        sshExec.setCommand(command);
        sshExec.setProject(PROJECT);
        sshExec.setTrust(true);
        log.info("Executing command: " + command + " on " + vmIpAddress);
        sshExec.execute();
    }
}
