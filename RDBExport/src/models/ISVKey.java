package models;

import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import enums.ClientProviderType;
import exceptions.IllegalKeyException;
import exceptions.IllegalRedisTypeException;

public class ISVKey extends BaseKey {

    public ISVKey(KeyValuePair<?, ?> parsedKV) throws IllegalKeyException, IllegalRedisTypeException {
        super(parsedKV);
    }

    @Override
    protected void parseKeyMetadata(KeyValuePair<?, ?> parsedKV) throws IllegalKeyException {
        String k = new String((byte[]) parsedKV.getKey());

        if (k.charAt(0) == '{') {
            String[] split = k.split("}");
            if (split.length != 2) {
                throw new IllegalKeyException(k);
            }

            String[] keyspaceAndTenantId = split[0].split("_");
            if (keyspaceAndTenantId.length != 4) {
                throw new IllegalKeyException(k);
            }

            this.keyspace = String.join("_", new String[] {"C", keyspaceAndTenantId[1], keyspaceAndTenantId[2]});
            this.tenantId = Integer.parseInt(keyspaceAndTenantId[2]);
            this.key = split[1];
            this.clientProvider = ClientProviderType.Native;
        } else {
            String[] split = k.split("_");
            if (split.length < 5) {
                throw new IllegalKeyException(k);
            }

            this.keyspace = String.join("_", new String[] {split[0], split[1], split[2]});
            this.tenantId = Integer.parseInt(split[2]);
            this.key = k.substring(split[0].length() + split[1].length() + split[2].length() + split[3].length() + 4);
            this.clientProvider = ClientProviderType.Random;
        }
    }
}
