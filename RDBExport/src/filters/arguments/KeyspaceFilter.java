package filters.arguments;

import filters.IFilter;
import models.Context;
import models.KeyInformation;

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
    public boolean doFilter(KeyInformation keyInformation, Context context) {
        if (Arrays.stream(context.getKeyspaces()).noneMatch(keyspace -> Objects.equals(keyspace, keyInformation.getKeyspace()))) {
            return false;
        }

        return next == null || next.doFilter(keyInformation, context);
    }
}
