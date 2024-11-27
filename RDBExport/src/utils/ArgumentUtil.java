package utils;


import common.ArgumentConstants;
import common.Context;

import java.util.Arrays;

public class ArgumentUtil {

    public static Context parseArgument(String[] arguments) {
        if (arguments == null || arguments.length == 0) {
            help();
            System.exit(0);
        }

        String[] keyspaces = ArgumentConstants.DEFAULT_ARGUMENT_KEYSPACES;
        int[] tenantIds = ArgumentConstants.DEFAULT_ARGUMENT_TENANTIDS;
        int bytes = ArgumentConstants.DEFAULT_ARGUMENT_BYTES;
        float days = ArgumentConstants.DEFAULT_ARGUMENT_DAYS;
        int elementsCount = ArgumentConstants.DEFAULT_ARGUMENT_ELEMENTS_COUNT;
        boolean neverExpire = ArgumentConstants.DEFAULT_ARGUMENT_NEVER_EXPIRE;
        String prefix = ArgumentConstants.DEFAULT_ARGUMENT_PREFIX;
        String suffix = ArgumentConstants.DEFAULT_ARGUMENT_SUFFIX;

        String[] rdbs = ArgumentConstants.DEFAULT_ARGUMENT_RDBS;
        String result = ArgumentConstants.DEFAULT_ARGUMENT_RESULT;
        boolean sorted = ArgumentConstants.DEFAULT_ARGUMENT_SORTED;
        int threads = ArgumentConstants.DEFAULT_ARGUMENT_THREADS;

        for (int i = 0; i < arguments.length; i++) {
            String argument = arguments[i].toLowerCase();

            switch (argument) {
                case "-keyspaces":
                    keyspaces = arguments[++i].split(",");
                    break;

                case "-tenantids":
                    tenantIds = Arrays.stream(arguments[++i].split(",")).mapToInt(Integer::parseInt).toArray();
                    break;

                case "-bytes":
                    bytes = Integer.parseInt(arguments[++i]);
                    break;

                case "-days":
                    days = Float.parseFloat(arguments[++i]);
                    break;

                case "-elementscount":
                    elementsCount = Integer.parseInt(arguments[++i]);
                    break;

                case "-neverexpire":
                    neverExpire = true;
                    break;

                case "-prefix":
                    prefix = arguments[++i];
                    break;

                case "-suffix":
                    suffix = arguments[++i];
                    break;

                case "-rdbs":
                    rdbs = arguments[++i].split(",");
                    break;

                case "-result":
                    result = arguments[++i];
                    break;

                case "-sorted":
                    sorted = true;
                    break;

                case "-threads":
                    threads = Integer.parseInt(arguments[++i]);
                    break;

                default:
                    help();
                    System.exit(-1);
            }
        }

        return new Context(keyspaces, tenantIds, bytes, days, elementsCount, neverExpire, prefix, suffix, rdbs, result, sorted, threads);
    }

    public static void help() {
        System.out.println("-keyspaces string");
        System.out.println("        Export multiple keyspaces.");

        System.out.println("-tenantIds int");
        System.out.println("        Export multiple tenantIds.");

        System.out.println("-bytes int");
        System.out.println("        Only output key's used memory equal or greater than this size (in byte).");

        System.out.println("-days float");
        System.out.println("        Only output key's expire days equal or greater than this value.");

        System.out.println("-elementsCount int");
        System.out.println("        Only output elements length equal or greater than this value (hash, set, zset, list).");

        System.out.println("-neverExpire");
        System.out.println("        Only output keys which do not contains expired time.");

        System.out.println("-prefix string");
        System.out.println("        Prefix.");

        System.out.println("-suffix string");
        System.out.println("        Suffix.");

        System.out.println("-rdbs string");
        System.out.println("        RDB files path.");

        System.out.println("-result string");
        System.out.println("        The result file's path in csv format.");

        System.out.println("-sorted");
        System.out.println("        Sort keys in descending order by memory.");

        System.out.println("-threads int");
        System.out.println("        Threads to parse rdb files (default 1).");
    }
}
