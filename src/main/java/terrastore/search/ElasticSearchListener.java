package terrastore.search;

import java.util.HashSet;
import java.util.Set;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.event.ActionExecutor;
import terrastore.event.Event;
import terrastore.event.EventListener;
import static java.util.Arrays.asList;
import static org.elasticsearch.client.Requests.indexRequest;

/**
 * @author kimchy (Shay Banon)
 * @author Sergio Bossa
 */
public class ElasticSearchListener implements EventListener {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchListener.class);
    //
    private final ElasticSearchServer server;
    private final IndexNameResolver indexNameResolver;
    private final boolean asyncOperations;
    private final Set<String> indexedBuckets;

    public ElasticSearchListener(ElasticSearchServer server, IndexNameResolver indexNameResolver, boolean asyncOperations) {
        this(server, indexNameResolver, asyncOperations, new String[]{});
    }

    public ElasticSearchListener(ElasticSearchServer server, IndexNameResolver indexNameResolver, boolean asyncOperations, String... indexedBuckets) {
        this.server = server;
        this.indexNameResolver = indexNameResolver;
        this.asyncOperations = asyncOperations;
        if (indexedBuckets == null) {
            this.indexedBuckets = new HashSet<String>();
        } else {
            this.indexedBuckets = new HashSet<String>(asList(indexedBuckets));
        }
    }

    @Override
    public void init() {
        server.start();
    }

    @Override
    public boolean observes(String bucket) {
        return indexedBuckets.isEmpty() || indexedBuckets.contains(bucket);
    }

    @Override
    public void onValueChanged(Event event, ActionExecutor executor) {
        final String bucket = event.getBucket();
        final String key = event.getKey();
        byte[] value = event.getNewValueAsBytes();
        String index = indexNameResolver.resolve(bucket);
        IndexRequest indexRequest = indexRequest(index).type(bucket).id(key).source(value);
        if (asyncOperations) {
            server.getClient().index(indexRequest, new ActionListener<IndexResponse>() {

                @Override
                public void onResponse(IndexResponse indexResponse) {
                    // all is well
                }

                @Override
                public void onFailure(Throwable t) {
                    LOG.warn("Failed to index [" + bucket + "][" + key + "]", t);
                }
            });
        } else {
            try {
                server.getClient().index(indexRequest).actionGet();
            } catch (ElasticSearchException e) {
                LOG.warn("Failed to index [" + bucket + "][" + key + "]", e);
            }
        }
    }

    @Override
    public void onValueRemoved(Event event, ActionExecutor executor) {
        final String bucket = event.getBucket();
        final String key = event.getKey();
        String index = indexNameResolver.resolve(bucket);
        DeleteRequest request = Requests.deleteRequest(index).type(bucket).id(key);
        if (asyncOperations) {
            server.getClient().delete(request, new ActionListener<DeleteResponse>() {

                @Override
                public void onResponse(DeleteResponse deleteResponse) {
                    // all is well
                }

                @Override
                public void onFailure(Throwable t) {
                    LOG.warn("Failed to delete [" + bucket + "][" + key + "]", t);
                }
            });
        } else {
            try {
                server.getClient().delete(request).actionGet();
            } catch (ElasticSearchException e) {
                LOG.warn("Failed to delete [" + bucket + "][" + key + "]", e);
            }
        }
    }

    @Override
    public void cleanup() {
        server.stop();
    }
}
