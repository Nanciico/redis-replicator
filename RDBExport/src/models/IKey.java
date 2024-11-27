package models;

import enums.ClientProviderType;
import enums.RedisType;

public interface IKey {

    RedisType getType();

    String getKeyspace();

    int getTenantId();

    String getKey();

    int getSizeInBytes();

    int getNumElements();

    ClientProviderType getClientProvider();

    Long getExpireDatetime();

    String getFormattedExpireDatetime();
}
