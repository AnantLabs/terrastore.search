package terrastore.search;

import java.util.Map;
import org.elasticsearch.client.Client;
import org.elasticsearch.server.Server;

import static org.elasticsearch.server.ServerBuilder.serverBuilder;
import static org.elasticsearch.util.settings.ImmutableSettings.settingsBuilder;

/**
 * @author kimchy (Shay Banon)
 * @author Sergio Bossa
 */
public class ElasticSearchServer {

    private final Map configuration;
    private Server server;

    public ElasticSearchServer(Map configuration) {
        this(configuration, true);
    }

    public ElasticSearchServer(Map configuration, boolean preferIPv4) {
        this.configuration = configuration;
        System.setProperty("java.net.preferIPv4Stack", Boolean.toString(preferIPv4));
    }

    public void start() {
        server = serverBuilder().settings(settingsBuilder().put(configuration)).build();
        server.start();
    }

    public void stop() {
        server.close();
    }

    public Client getClient() {
        return server.client();
    }
}
