package terrastore.search;

/**
 * @author Sergio Bossa
 */
public interface IndexNameResolver {

    public String resolve(String bucket);
}
