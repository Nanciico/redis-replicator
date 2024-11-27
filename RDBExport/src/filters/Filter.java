package filters;

import common.Context;
import models.IKey;

public class Filter implements IFilter{

    private IFilter next = null;

    @Override
    public IFilter setNext(IFilter filter) {
        this.next = filter;
        return filter;
    }

    @Override
    public boolean doFilter(IKey key, Context context) {
        return next == null || next.doFilter(key, context);
    }
}
