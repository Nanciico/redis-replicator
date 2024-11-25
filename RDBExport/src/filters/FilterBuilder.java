package filters;

import common.ArgumentConstants;
import filters.arguments.*;
import models.Context;

import java.util.Objects;

public class FilterBuilder {

    public static Filter buildFilter(Context context) {
        Filter filter = new Filter();

        IFilter current = filter;
        if (context.getKeyspaces() != ArgumentConstants.DEFAULT_ARGUMENT_KEYSPACES) {
            current = current.setNext(new KeyspaceFilter());
        }

        if (context.getTenantIds() != ArgumentConstants.DEFAULT_ARGUMENT_TENANTIDS) {
            current = current.setNext(new TenantIdFilter());
        }

        if (context.getBytes() != ArgumentConstants.DEFAULT_ARGUMENT_BYTES) {
            current = current.setNext(new BytesFilter());
        }

        if (context.getDays() != ArgumentConstants.DEFAULT_ARGUMENT_DAYS) {
            current = current.setNext(new DaysFilter());
        }

        if (context.getElementsCount() != ArgumentConstants.DEFAULT_ARGUMENT_ELEMENTS_COUNT) {
            current = current.setNext(new ElementsCountFilter());
        }

        if (context.isNeverExpire() != ArgumentConstants.DEFAULT_ARGUMENT_NEVER_EXPIRE) {
            current = current.setNext(new NeverExpireFilter());
        }

        if (!Objects.equals(context.getPrefix(), ArgumentConstants.DEFAULT_ARGUMENT_PREFIX)) {
            current = current.setNext(new PrefixFilter());
        }

        if (!Objects.equals(context.getSuffix(), ArgumentConstants.DEFAULT_ARGUMENT_SUFFIX)) {
            current = current.setNext(new SuffixFilter());
        }

        return filter;
    }
}
