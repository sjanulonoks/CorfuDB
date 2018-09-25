package org.corfudb.universe.node;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.corfudb.universe.node.CorfuServer.ServerParams;
import org.corfudb.universe.universe.UniverseException;

import java.io.FileReader;
import java.io.IOException;

@Slf4j
public abstract class AbstractCorfuServer<T extends ServerParams> implements CorfuServer {
    public static final String ALL_NETWORK_INTERFACES = "0.0.0.0";

    @Getter
    protected final T params;

    protected AbstractCorfuServer(T params) {
        this.params = params;
    }


    /**
     * This method create a command line string for starting Corfu server
     *
     * @return command line parameters
     */
    protected String getCommandLineParams() {
        StringBuilder cmd = new StringBuilder();
        cmd.append("-a ").append(ALL_NETWORK_INTERFACES);

        switch (params.getPersistence()) {
            case DISK:
                if (StringUtils.isEmpty(params.getLogDir())) {
                    throw new UniverseException("Invalid log dir in disk persistence mode");
                }
                cmd.append(" -l ").append(params.getLogDir());
                break;
            case MEMORY:
                cmd.append(" -m");
                break;
        }

        if (params.getMode() == Mode.SINGLE) {
            cmd.append(" -s");
        }

        cmd.append(" -d ").append(params.getLogLevel().toString()).append(" ");

        cmd.append(params.getPort());

        String cmdLineParams = cmd.toString();
        log.trace("Command line parameters: {}", cmdLineParams);

        return cmdLineParams;
    }

    protected static String getAppVersion() {
        MavenXpp3Reader reader = new MavenXpp3Reader();
        Model model;
        try {
            model = reader.read(new FileReader("pom.xml"));
            return model.getParent().getVersion();
        } catch (IOException | XmlPullParserException e) {
            throw new NodeException("Can't parse application version", e);
        }
    }
}
