package terrastore.search;

import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final ThreadLocal<UnicodeUtil.UTF16Result> cachedUtf = new ThreadLocal<UnicodeUtil.UTF16Result>() {

        @Override
        protected UnicodeUtil.UTF16Result initialValue() {
            return new UnicodeUtil.UTF16Result();
        }
    };
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
    public void onValueChanged(final String bucket, final String key, final byte[] value) {
        UnicodeUtil.UTF16Result utf16 = cachedUtf.get();
        UnicodeUtil.UTF8toUTF16(value, 0, value.length, utf16);
        String source = new String(utf16.result, 0, utf16.length);

        String index = indexNameResolver.resolve(bucket);
        IndexRequest indexRequest = indexRequest(index).type(bucket).id(key).source(source);
        if (asyncOperations) {
            server.getClient().execIndex(indexRequest, new ActionListener<IndexResponse>() {

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
    public void onValueRemoved(final String bucket, final String key) {
        String index = indexNameResolver.resolve(bucket);
        DeleteRequest request = Requests.deleteRequest(index).type(bucket).id(key);
        if (asyncOperations) {
            server.getClient().execDelete(request, new ActionListener<DeleteResponse>() {

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
