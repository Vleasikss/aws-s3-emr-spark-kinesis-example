package org.example;

public enum WarehouseType {
    /**
     * The smallest&cheapest Snowflake Warehouse instance
     */
    X_SMALL("x-small");

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
