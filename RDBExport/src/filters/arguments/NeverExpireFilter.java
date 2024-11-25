package filters.arguments;

import filters.IFilter;
import models.Context;
import models.KeyInformation;

public class NeverExpireFilter implements IFilter {

    private IFilter next = null;

    @Override
    public IFilter setNext(IFilter filter) {
        this.next = filter;
        return filter;
    }

    @Override
    public boolean doFilter(KeyInformation keyInformation, Context context) {
        if (keyInformation.getExpireDatetime() != null) {
            return false;
        }

        return next == null || next.doFilter(keyInformation, context);
    }
}
