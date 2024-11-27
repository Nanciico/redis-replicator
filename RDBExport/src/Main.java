import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.FileType;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import com.moilioncircle.redis.replicator.rdb.dump.DumpRdbVisitor;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.dump.parser.DefaultDumpValueParser;
import com.moilioncircle.redis.replicator.rdb.dump.parser.DumpValueParser;
import exceptions.IllegalKeyException;
import exceptions.IllegalRedisTypeException;
import filters.FilterBuilder;
import filters.IFilter;
import io.OutputWriter;
import models.IKey;
import models.KeyFactory;
import utils.ArgumentUtil;
import common.Context;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        Context context = ArgumentUtil.parseArgument(args);

        String[] rdbs = context.getRdbs();

        OutputWriter.buildOutputWriter(context.getResult());

        List<IKey> readyToOutput = Collections.synchronizedList(new LinkedList<>());

        ExecutorService executorService = Executors.newFixedThreadPool(context.getThreads());
        ArrayList<Future<?>> futures = new ArrayList<>();
        for (String rdb : rdbs) {
            Future<?> future = executorService.submit(() -> {
                try {
                    parseRdb(rdb, context, readyToOutput);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            futures.add(future);
        }
        for (Future<?> future : futures) {
            future.get();
        }

        readyToOutput.sort((o1, o2) -> Integer.compare(o2.getSizeInBytes(), o1.getSizeInBytes()));
        OutputWriter.writeAll(readyToOutput);

        System.exit(0);
    }

    private static void parseRdb(String rdbPathname, Context context, List<IKey> readyToOutput) throws IOException {
        IFilter filter = FilterBuilder.buildFilter(context);

        Replicator redisReplicator = new RedisReplicator(new File(rdbPathname), FileType.RDB, Configuration.defaultSetting());
        redisReplicator.setRdbVisitor(new DumpRdbVisitor(redisReplicator));
        DumpValueParser parser = new DefaultDumpValueParser(redisReplicator);
        redisReplicator.addEventListener((replicator, event) -> {
            if (event instanceof DumpKeyValuePair) {
                KeyValuePair<?, ?> parsedKV = parser.parse((DumpKeyValuePair) event);

                try {
                    IKey key = KeyFactory.createKey(parsedKV);

                    if (filter.doFilter(key, context)) {
                        if (context.isSorted()) {
                            readyToOutput.add(key);
                        } else {
                            OutputWriter.writeNext(key);
                        }
                    }
                }catch (IllegalKeyException e) {
                    System.out.println("Illegal Redis key. key: [" + e.getKey() + "].");
                } catch (IllegalRedisTypeException e) {
                    System.out.println("Illegal Redis type. key: [" + e.getKey() + "] type: [" + e.getType() + "].");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        redisReplicator.open();
    }
}
