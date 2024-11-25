package exceptions;

public class IllegalKeyException extends Throwable {

    private final String key;

    public IllegalKeyException(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
