package filters.arguments;

import filters.IFilter;
import models.Context;
import models.KeyInformation;

import java.util.Arrays;
import java.util.Objects;

public class TenantIdFilter implements IFilter {

    private IFilter next = null;

    @Override
    public IFilter setNext(IFilter filter) {
        this.next = filter;
        return filter;
    }

    @Override
    public boolean doFilter(KeyInformation keyInformation, Context context) {
        if (Arrays.stream(context.getTenantIds()).noneMatch(tenantId -> Objects.equals(tenantId, keyInformation.getTenantId()))) {
            return false;
        }

        return next == null || next.doFilter(keyInformation, context);
    }
}
