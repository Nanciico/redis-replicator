package filters.arguments;

import filters.IFilter;
import common.Context;
import models.IKey;

public class ElementsCountFilter implements IFilter {

    private IFilter next = null;

    @Override
    public IFilter setNext(IFilter filter) {
        this.next = filter;
        return filter;
    }

    @Override
    public boolean doFilter(IKey key, Context context) {
        if (context.getElementsCount() > key.getNumElements()) {
            return false;
        }

        return next == null || next.doFilter(key, context);
    }
}
