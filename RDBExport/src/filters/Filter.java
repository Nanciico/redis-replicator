package filters;

import models.Context;
import models.KeyInformation;

public class Filter implements IFilter{

    private IFilter next = null;

    @Override
    public IFilter setNext(IFilter filter) {
        this.next = filter;
        return filter;
    }

    @Override
    public boolean doFilter(KeyInformation keyInformation, Context context) {
        return next == null || next.doFilter(keyInformation, context);
    }
}
