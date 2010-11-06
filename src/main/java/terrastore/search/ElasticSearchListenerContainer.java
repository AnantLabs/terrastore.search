package terrastore.search;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import terrastore.annotation.AutoDetect;
import terrastore.event.ActionExecutor;
import terrastore.event.Event;
import terrastore.event.EventListener;

/**
 * @author Sergio Bossa
 */
@AutoDetect(name = "searchListener", order = 5)
public class ElasticSearchListenerContainer implements EventListener {

    private final static String TERRASTORE_SEARCH_CONTEXT = "terrastore-search-extension.xml";
    private final ElasticSearchListener delegate;

    public ElasticSearchListenerContainer() {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(TERRASTORE_SEARCH_CONTEXT);
        delegate = context.getBean(ElasticSearchListener.class);
    }

    @Override
    public boolean observes(String bucket) {
        return delegate.observes(bucket);
    }

    @Override
    public void onValueChanged(Event event, ActionExecutor executor) {
        delegate.onValueChanged(event, executor);
    }

    @Override
    public void onValueRemoved(Event event, ActionExecutor executor) {
        delegate.onValueRemoved(event, executor);
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
