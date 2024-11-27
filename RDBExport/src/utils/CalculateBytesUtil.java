package utils;

import com.moilioncircle.redis.replicator.rdb.datatype.ZSetEntry;
import com.moilioncircle.redis.replicator.util.ByteArrayList;
import com.moilioncircle.redis.replicator.util.ByteArrayMap;
import com.moilioncircle.redis.replicator.util.ByteArraySet;

import java.util.LinkedHashSet;
import java.util.Map;

public class CalculateBytesUtil {

    public static int calculateBytes(ByteArraySet byteArraySet) {
        int bytes = 0;
        for (byte[] entry : byteArraySet) {
            bytes += entry.length;
        }
        return bytes;
    }

    public static int calculateBytes(ByteArrayMap byteArrayMap) {
        int bytes = 0;
        for (Map.Entry<byte[], byte[]> entry : byteArrayMap.entrySet()) {
            bytes += entry.getKey().length;
            bytes += entry.getValue().length;
        }
        return bytes;
    }

    public static int calculateBytes(ByteArrayList byteArrayList) {
        int bytes = 0;
        for (byte[] element : byteArrayList) {
            bytes += element.length;
        }
        return bytes;
    }

    public static int calculateBytes(LinkedHashSet<ZSetEntry> linkedHashSet) {
        int bytes = 0;
        for (ZSetEntry entry : linkedHashSet) {
            bytes += entry.getElement().length;
            bytes += 8;
        }
        return bytes;
    }
}
