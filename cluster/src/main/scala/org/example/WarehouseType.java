package org.example;

/**
 * Pay only for queries (not hourly).
 * Each warehouse is independent of each other.
 *
 * Snowflake works really fast and effective with many small tasks.
 */
public enum WarehouseType {

    /**
     * Cost: 1 credit per hour
     */
    X_SMALL("x-small"),

    /**
     * Cost: 2 credits per hour
     */
    SMALL("small"),

    /**
     * Cost: 4 credits per hour
     */
    MEDIUM("medium"),

    /**
     * Cost: 8 credits per hour
     */
    LARGE("large"),

    /**
     * Cost: 16 credits per hour
     */
    X_LARGE("x-large"),

    /**
     * Cost: 32 credits per hour
     */
    X2_LARGE("2x-large"),

    /**
     * Cost: 64 credits per hour
     */
    X3_LARGE("3x-large"),

    /**
     * Cost: 128 credits per hour
     */
    X4_LARGE("4x-large");

    private final String value;

    WarehouseType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "WarehouseType{" +
                "value='" + value + '\'' +
                '}';
    }
}
