package terrastore.search;

import java.util.Properties;
import org.elasticsearch.action.get.GetRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class ElasticSearchTest {

    private static final String INDEX = "search";
    private static final String BUCKET = "bucket";
    private volatile ElasticSearchServer server;
    private volatile ElasticSearchListenerContainer listener;

    @Before
    public void setUp() {
        server = new ElasticSearchServer(new Properties());
        listener = new ElasticSearchListenerContainer();
        server.start();
        listener.init();
    }

    @After
    public void tearDown() {
        listener.cleanup();
        server.stop();
    }

    @Test
    public void testOnValueChanged() throws Exception {
        String key = "key";
        String value = "{\"key\":\"value\"}";

        listener.onValueChanged(BUCKET, key, value.getBytes("UTF-8"));
        //
        Thread.sleep(3000);
        //
        assertEquals(value, server.getClient().get(new GetRequest(INDEX, BUCKET, key)).actionGet().sourceAsString());
    }

    @Test
    public void testOnValueChangedAndRemoved() throws Exception {
        String key = "key";
        String value = "{\"key\":\"value\"}";

        listener.onValueChanged(BUCKET, key, value.getBytes("UTF-8"));
        //
        Thread.sleep(3000);
        //
        assertEquals(value, server.getClient().get(new GetRequest(INDEX, BUCKET, key)).actionGet().sourceAsString());
        //
        listener.onValueRemoved(BUCKET, key);
        //
        Thread.sleep(3000);
        //
        assertNull(server.getClient().get(new GetRequest(INDEX, BUCKET, key)).actionGet().source());
    }
}
