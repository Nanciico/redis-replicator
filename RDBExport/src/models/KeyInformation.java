package models;

import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueHash;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueList;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.ZSetEntry;
import com.moilioncircle.redis.replicator.util.ByteArrayList;
import com.moilioncircle.redis.replicator.util.ByteArrayMap;
import com.moilioncircle.redis.replicator.util.ByteArraySet;
import enums.ClientProviderType;
import enums.RedisType;
import exceptions.IllegalKeyException;
import exceptions.IllegalRedisTypeException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Map;

import static com.moilioncircle.redis.replicator.Constants.*;

public class KeyInformation {

    private RedisType type;

    private String keyspace;

    private int tenantId;

    private String key;

    private int sizeInBytes;

    private int numElements;

    private ClientProviderType clientProvider;

    private Long expireDatetime;


    public KeyInformation(KeyValuePair<?, ?> parsedKV) throws IllegalKeyException, IllegalRedisTypeException {
        parseKeyMetadata(parsedKV);
        parseValueMetadata(parsedKV);
    }

    private void parseValueMetadata(KeyValuePair<?, ?> parsedKV) throws IllegalRedisTypeException {
        switch (parsedKV.getValueRdbType()) {
            case RDB_TYPE_STRING:
                this.type = RedisType.String;
                this.sizeInBytes = ((byte[]) parsedKV.getValue()).length;
                this.numElements = 0;
                this.expireDatetime = parsedKV.getExpiredMs();
                break;

            case RDB_TYPE_LIST:
            case RDB_TYPE_LIST_ZIPLIST:
            case RDB_TYPE_LIST_QUICKLIST:
            case RDB_TYPE_LIST_QUICKLIST_2:
                this.type = RedisType.List;
                ByteArrayList valueBytes1 = (ByteArrayList) parsedKV.getValue();
                this.sizeInBytes = calculateBytes(valueBytes1);
                this.numElements = valueBytes1.size();
                this.expireDatetime = parsedKV.getExpiredMs();
                break;

            case RDB_TYPE_SET:
            case RDB_TYPE_SET_INTSET:
            case RDB_TYPE_SET_LISTPACK:
                this.type = RedisType.Set;
                ByteArraySet valueBytes2 = (ByteArraySet) parsedKV.getValue();
                this.sizeInBytes = calculateBytes(valueBytes2);
                this.numElements = valueBytes2.size();
                this.expireDatetime = parsedKV.getExpiredMs();
                break;

            case RDB_TYPE_ZSET:
            case RDB_TYPE_ZSET_2:
            case RDB_TYPE_ZSET_ZIPLIST:
            case RDB_TYPE_ZSET_LISTPACK:
                this.type = RedisType.ZSet;
                LinkedHashSet<ZSetEntry> valueBytes3 = (LinkedHashSet<ZSetEntry>) parsedKV.getValue();
                this.sizeInBytes = calculateBytes(valueBytes3);
                this.numElements = valueBytes3.size();
                this.expireDatetime = parsedKV.getExpiredMs();
                break;

            case RDB_TYPE_HASH:
            case RDB_TYPE_HASH_ZIPMAP:
            case RDB_TYPE_HASH_ZIPLIST:
            case RDB_TYPE_HASH_LISTPACK:
                this.type = RedisType.Hash;
                ByteArrayMap valueBytes4 = (ByteArrayMap) parsedKV.getValue();
                this.sizeInBytes = calculateBytes(valueBytes4);
                this.numElements = valueBytes4.size();
                this.expireDatetime = parsedKV.getExpiredMs();
                break;

            default:
                throw new IllegalRedisTypeException(new String((byte[]) parsedKV.getKey()), parsedKV.getValueRdbType());
        }
    }

    private int calculateBytes(ByteArraySet byteArraySet) {
        int bytes = 0;
        for (byte[] entry : byteArraySet) {
            bytes += entry.length;
        }
        return bytes;
    }

    private int calculateBytes(ByteArrayMap byteArrayMap) {
        int bytes = 0;
        for (Map.Entry<byte[], byte[]> entry : byteArrayMap.entrySet()) {
            bytes += entry.getKey().length;
            bytes += entry.getValue().length;
        }
        return bytes;
    }

    private int calculateBytes(ByteArrayList byteArrayList) {
        int bytes = 0;
        for (byte[] element : byteArrayList) {
            bytes += element.length;
        }
        return bytes;
    }

    private int calculateBytes(LinkedHashSet<ZSetEntry> linkedHashSet) {
        int bytes = 0;
        for (ZSetEntry entry : linkedHashSet) {
            bytes += entry.getElement().length;
            bytes += 8;
        }
        return bytes;
    }

    private void parseKeyMetadata(KeyValuePair<?, ?> parsedKV) throws IllegalKeyException {
        String k = new String((byte[]) parsedKV.getKey());

        if (k.charAt(0) == '{') {
            String[] split = k.split("}");
            if (split.length != 2) {
                throw new IllegalKeyException(k);
            }

            String[] keyspaceAndTenantId = split[0].split("_");
            if (keyspaceAndTenantId.length != 2) {
                throw new IllegalKeyException(k);
            }

            this.keyspace = keyspaceAndTenantId[0].substring(1);
            this.tenantId = Integer.parseInt(keyspaceAndTenantId[1]);
            this.key = split[1];
            this.clientProvider = ClientProviderType.Native;
        } else {
            String[] split = k.split("_");
            if (split.length < 3) {
                throw new IllegalKeyException(k);
            }

            this.keyspace = split[0];
            this.tenantId = Integer.parseInt(split[1]);
            this.key = k.substring(split[0].length() + split[1].length() + 2);
            this.clientProvider = ClientProviderType.Random;
        }
    }

    public RedisType getType() {
        return type;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public int getTenantId() {
        return tenantId;
    }

    public String getKey() {
        return key;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    public int getNumElements() {
        return numElements;
    }

    public ClientProviderType getClientProvider() {
        return clientProvider;
    }

    public Long getExpireDatetime() {
        return expireDatetime;
    }

    public String getFormattedExpireDatetime() {

        return expireDatetime == null ? null : new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(new Date(expireDatetime));
    }
}
