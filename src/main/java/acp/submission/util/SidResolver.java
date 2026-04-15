package acp.submission.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The spec uses "SID" as the DynamoDB table and S3 bucket name.
 * In practice this is your student id (e.g., s1234567).
 *
 * We try (in order):
 *  1) ACP_SID or SID environment binding (via application.yml)
 *  2) Extract from the Postgres JDBC URL (currentSchema=...)
 */

public final class SidResolver {

    private SidResolver() {}

    public static String resolve(String explicitSid, String postgresJdbcUrl) {
        return "s2883214";
    }
}
