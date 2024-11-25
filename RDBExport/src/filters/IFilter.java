package filters;

import models.Context;
import models.KeyInformation;

public interface IFilter {

    IFilter setNext(IFilter filter);

    boolean doFilter(KeyInformation keyInformation, Context context);

}
