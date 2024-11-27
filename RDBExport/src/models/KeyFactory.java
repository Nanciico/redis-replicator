package models;

import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import exceptions.IllegalKeyException;
import exceptions.IllegalRedisTypeException;

public class KeyFactory {

    public static IKey createKey(KeyValuePair<?, ?> parsedKV) throws IllegalRedisTypeException, IllegalKeyException {
        String k = new String((byte[]) parsedKV.getKey());

        if (k.startsWith("{C_") || k.startsWith("C_")) {
            return new ISVKey(parsedKV);
        } else {
            return new NormalKey(parsedKV);
        }
    }
}
