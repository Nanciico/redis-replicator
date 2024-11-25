package models;

public class Context {

    private final String[] keyspaces;

    private final int[] tenantIds;

    private final int bytes;

    private final float days;

    private final int elementsCount;

    private final boolean neverExpire;

    private final String prefix;

    private final String suffix;


    private final String[] rdbs;

    private final String result;

    private final boolean sorted;


    private final int threads;

    public Context(String[] keyspaces, int[] tenantIds, int bytes, float days, int elementsCount, boolean neverExpire,
                   String prefix, String suffix, String[] rdbs, String result, boolean sorted, int threads) {
        this.keyspaces = keyspaces;
        this.tenantIds = tenantIds;
        this.bytes = bytes;
        this.days = days;
        this.elementsCount = elementsCount;
        this.neverExpire = neverExpire;
        this.prefix = prefix;
        this.suffix = suffix;

        this.rdbs = rdbs;
        this.result = result;
        this.sorted = sorted;

        this.threads = threads;
    }


    public String[] getKeyspaces() {
        return keyspaces;
    }

    public int[] getTenantIds() {
        return tenantIds;
    }

    public int getBytes() {
        return bytes;
    }

    public float getDays() {
        return days;
    }

    public int getElementsCount() {
        return elementsCount;
    }

    public boolean isNeverExpire() {
        return neverExpire;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getSuffix() {
        return suffix;
    }

    public String[] getRdbs() {
        return rdbs;
    }

    public String getResult() {
        return result;
    }

    public boolean isSorted() {
        return sorted;
    }

    public int getThreads() {
        return threads;
    }
}
