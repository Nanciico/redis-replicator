package models;

import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.ZSetEntry;
import com.moilioncircle.redis.replicator.util.ByteArrayList;
import com.moilioncircle.redis.replicator.util.ByteArrayMap;
import com.moilioncircle.redis.replicator.util.ByteArraySet;
import enums.ClientProviderType;
import enums.RedisType;
import exceptions.IllegalKeyException;
import exceptions.IllegalRedisTypeException;
import utils.CalculateBytesUtil;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashSet;

import static com.moilioncircle.redis.replicator.Constants.*;

public abstract class BaseKey implements IKey {

    protected RedisType type;

    protected String keyspace;

    protected int tenantId;

    protected String key;

    protected int sizeInBytes;

    protected int numElements;

    protected ClientProviderType clientProvider;

    protected Long expireDatetime;

    public BaseKey(KeyValuePair<?, ?> parsedKV) throws IllegalKeyException, IllegalRedisTypeException {
        parseKeyMetadata(parsedKV);
        parseValueMetadata(parsedKV);
    }

    protected abstract void parseKeyMetadata(KeyValuePair<?, ?> parsedKV) throws IllegalKeyException;

    protected final void parseValueMetadata(KeyValuePair<?, ?> parsedKV) throws IllegalRedisTypeException {
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
                this.sizeInBytes = CalculateBytesUtil.calculateBytes(valueBytes1);
                this.numElements = valueBytes1.size();
                this.expireDatetime = parsedKV.getExpiredMs();
                break;

            case RDB_TYPE_SET:
            case RDB_TYPE_SET_INTSET:
            case RDB_TYPE_SET_LISTPACK:
                this.type = RedisType.Set;
                ByteArraySet valueBytes2 = (ByteArraySet) parsedKV.getValue();
                this.sizeInBytes = CalculateBytesUtil.calculateBytes(valueBytes2);
                this.numElements = valueBytes2.size();
                this.expireDatetime = parsedKV.getExpiredMs();
                break;

            case RDB_TYPE_ZSET:
            case RDB_TYPE_ZSET_2:
            case RDB_TYPE_ZSET_ZIPLIST:
            case RDB_TYPE_ZSET_LISTPACK:
                this.type = RedisType.ZSet;
                LinkedHashSet<ZSetEntry> valueBytes3 = (LinkedHashSet<ZSetEntry>) parsedKV.getValue();
                this.sizeInBytes = CalculateBytesUtil.calculateBytes(valueBytes3);
                this.numElements = valueBytes3.size();
                this.expireDatetime = parsedKV.getExpiredMs();
                break;

            case RDB_TYPE_HASH:
            case RDB_TYPE_HASH_ZIPMAP:
            case RDB_TYPE_HASH_ZIPLIST:
            case RDB_TYPE_HASH_LISTPACK:
            case RDB_TYPE_HASH_METADATA:
            case RDB_TYPE_HASH_LISTPACK_EX:
                this.type = RedisType.Hash;
                ByteArrayMap valueBytes4 = (ByteArrayMap) parsedKV.getValue();
                this.sizeInBytes = CalculateBytesUtil.calculateBytes(valueBytes4);
                this.numElements = valueBytes4.size();
                this.expireDatetime = parsedKV.getExpiredMs();
                break;

            default:
                throw new IllegalRedisTypeException(new String((byte[]) parsedKV.getKey()), parsedKV.getValueRdbType());
        }
    }

    public String getFormattedExpireDatetime() {
        return expireDatetime == null ? null : new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(new Date(expireDatetime));
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
}
