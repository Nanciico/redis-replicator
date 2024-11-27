package filters.arguments;

import filters.IFilter;
import common.Context;
import models.IKey;

import java.util.Arrays;
import java.util.Objects;

public class KeyspaceFilter implements IFilter {

    private IFilter next = null;

    @Override
    public IFilter setNext(IFilter filter) {
        this.next = filter;
        return filter;
    }

    @Override
    public boolean doFilter(IKey key, Context context) {
        if (Arrays.stream(context.getKeyspaces()).noneMatch(keyspace -> Objects.equals(keyspace, key.getKeyspace()))) {
            return false;
        }

        return next == null || next.doFilter(key, context);
    }
}
