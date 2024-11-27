package filters;

import common.Context;
import models.IKey;

public interface IFilter {

    IFilter setNext(IFilter filter);

    boolean doFilter(IKey key, Context context);
}
