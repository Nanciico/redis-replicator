package models;

import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import enums.ClientProviderType;
import exceptions.IllegalKeyException;
import exceptions.IllegalRedisTypeException;

public class NormalKey extends BaseKey {

    public NormalKey(KeyValuePair<?, ?> parsedKV) throws IllegalKeyException, IllegalRedisTypeException {
        super(parsedKV);
    }

    protected void parseKeyMetadata(KeyValuePair<?, ?> parsedKV) throws IllegalKeyException {
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
}
