package org.example;

/**
 * @see <a href="https://docs.snowflake.com/en/user-guide/security-access-control-overview.html">Snowflake roles</a>
 */
public enum DefaultSnowflakeRole {

    ACCOUNT_ADMIN("ACCOUNTADMIN"),
    SYS_ADMIN("SYSADMIN"),
    PUBLIC("PUBLIC"),
    SECURITY_ADMIN("SECURITYADMIN"),
    USER_ADMIN("USERADMIN");

    private final String value;

    DefaultSnowflakeRole(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public String toString() {
        return this.value;
    }
}
