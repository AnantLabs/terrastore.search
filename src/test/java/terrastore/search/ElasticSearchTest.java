package terrastore.search;

import java.util.Properties;
import org.elasticsearch.action.get.GetRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import terrastore.event.ActionExecutor;
import terrastore.event.Event;
import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class ElasticSearchTest {

    private static final String INDEX = "search";
    private static final String BUCKET = "bucket";
    private static final String KEY = "key";
    private static final String VALUE = "{\"key\":\"value\"}";
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
        Event event = createMock(Event.class);
        ActionExecutor executor = createMock(ActionExecutor.class);
        expect(event.getBucket()).andReturn(BUCKET).once();
        expect(event.getKey()).andReturn(KEY).once();
        expect(event.getNewValueAsBytes()).andReturn(VALUE.getBytes()).once();

        replay(event, executor);

        listener.onValueChanged(event, executor);
        //
        Thread.sleep(3000);
        //
        assertEquals(VALUE, server.getClient().get(new GetRequest(INDEX, BUCKET, KEY)).actionGet().sourceAsString());

        verify(event, executor);
    }

    @Test
    public void testOnValueChangedAndRemoved() throws Exception {
        Event event = createMock(Event.class);
        ActionExecutor executor = createMock(ActionExecutor.class);
        expect(event.getBucket()).andReturn(BUCKET).times(2);
        expect(event.getKey()).andReturn(KEY).times(2);
        expect(event.getNewValueAsBytes()).andReturn(VALUE.getBytes()).once();

        replay(event, executor);

        listener.onValueChanged(event, executor);
        //
        Thread.sleep(3000);
        //
        assertEquals(VALUE, server.getClient().get(new GetRequest(INDEX, BUCKET, KEY)).actionGet().sourceAsString());
        //
        listener.onValueRemoved(event, executor);
        //
        Thread.sleep(3000);
        //
        assertNull(server.getClient().get(new GetRequest(INDEX, BUCKET, KEY)).actionGet().source());

        verify(event, executor);
    }
}
