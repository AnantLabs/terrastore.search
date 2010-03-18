package terrastore.search;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import terrastore.annotation.AutoDetect;
import terrastore.event.EventListener;

/**
 * @author Sergio Bossa
 */
@AutoDetect(name = "searchListener", order = 5)
public class ElasticSearchListenerContainer implements EventListener {

    private final static String TERRASTORE_SEARCH_CONTEXT = "terrastore-search.xml";
    private final static String TERRASTORE_SEARCH_LISTENER = "searchListener";
    private final ElasticSearchListener delegate;

    public ElasticSearchListenerContainer() {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(TERRASTORE_SEARCH_CONTEXT);
        delegate = (ElasticSearchListener) context.getBean(TERRASTORE_SEARCH_LISTENER, ElasticSearchListener.class);
    }

    @Override
    public boolean observes(String bucket) {
        return delegate.observes(bucket);
    }

    @Override
    public void onValueChanged(final String bucket, final String key, final byte[] value) {
        delegate.onValueChanged(bucket, key, value);
    }

    @Override
    public void onValueRemoved(final String bucket, final String key) {
        delegate.onValueRemoved(bucket, key);
    }

    @Override
    public void init() {
        delegate.init();
    }

    @Override
    public void cleanup() {
        delegate.cleanup();
    }
}
