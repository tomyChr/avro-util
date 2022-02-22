package ch.ichristen.avroUtil.serde;

import org.apache.kafka.streams.state.KeyValueIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class KeyValueIteratorWrapper<K,V> implements AutoCloseable, Closeable, Iterator<V> {

    private final KeyValueIterator<K,V> internal;
    private final int pageSize;
    // first page = 0
    private int page;
    // first index = 0
    private int index = 0;
    private int currentPage = 0;

    private boolean closeable;

    /**
     * Create a new AbstractAvroHttpMessageConverter.
     * @param iterator the configured iterator to be wrapped
     * @param startPage the page to be returned (first page = 0)
     * @param pageSize the number of records per page
     * @param closeable if true, the wrapped iterator will be closed by the serializer
     */
    public KeyValueIteratorWrapper(KeyValueIterator<K,V> iterator, int startPage, int pageSize, boolean closeable) {
        this.internal = iterator;
        this.page = startPage;
        this.pageSize = pageSize;
        this.closeable = closeable;
    }

    @Override
    public void close() throws IOException {
        if (closeable) internal.close();
    }

    public void close(boolean forced) throws IOException {
        if (forced) {
            internal.close();
        } else {
            close();
        }
    }

    @Override
    public boolean hasNext() {
        assureCurrentPage();
        if (index < pageSize) return internal.hasNext();
        return false;
    }

    @Override
    public V next() {
        assureCurrentPage();
        if (index >= pageSize) throw new IndexOutOfBoundsException("iterator exceeded page size");
        index++;
        return internal.next().value;
    }

    public int getPage() { return page; }
    public void setPage(int page) {
        if (page < this.page) throw new IllegalArgumentException("New page (" + page + ") must not be smaller than current page (" + this.page + ")");
        this.page = page;
        this.index = 0;
    }

    public int getPageSize() { return pageSize; }

    public boolean isCloseable() { return closeable; }
    public void setCloseable( boolean closable) { this.closeable = closable; }

    private void assureCurrentPage() {
        if (currentPage != page ) {
            final long startOfPage = page * pageSize;
            long currentIndex = 0;
            while (internal.hasNext() && currentIndex < startOfPage) {
                internal.next();
                currentIndex++;
            }
            currentPage = page;
        }
    }
}
