package exceptions;

public class IllegalRedisTypeException extends Throwable {

    private final String key;

    private final int type;

    public IllegalRedisTypeException(String key, int type) {
        this.key = key;
        this. type = type;
    }

    public String getKey() {
        return key;
    }

    public int getType() {
        return type;
    }
}
