package terrastore.search;

/**
 * @author Sergio Bossa
 */
public class FixedIndexNameResolver implements IndexNameResolver {

    private final String indexName;

    public FixedIndexNameResolver(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public String resolve(String bucket) {
        return indexName;
    }
}
