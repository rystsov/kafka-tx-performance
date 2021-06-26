package io.vectorized.tests;

public class Consts 
{
    public static String KAFKA_CONNECTION_PROPERTY = "KAFKA_CONNECTION";
    public static String TXES_PROPERTY = "TXES";
    public static String WARMUP_PROPERTY = "WARMUP";
    public static String MEASURES_PROPERTY = "MEASURES";
    
    public static String topic1 = "topic1";
    public static String topic2 = "topic2";
    public static String txId1 = "my-tx-1";
    public static String txId2 = "my-tx-2";
    public static String groupId = "groupId";

    public static String getConnection() {
        var env = System.getenv();
        if (env.containsKey(KAFKA_CONNECTION_PROPERTY)) {
            return env.get(KAFKA_CONNECTION_PROPERTY);
        } else {
            return "127.0.0.1:9092";
        }
    }

    public static String getMeasuresFileName() {
        var env = System.getenv();
        if (env.containsKey(MEASURES_PROPERTY)) {
            return env.get(MEASURES_PROPERTY);
        } else {
            return "measures.log";
        }
    }

    public static int getTxes(int txes) {
        var env = System.getenv();
        if (env.containsKey(TXES_PROPERTY)) {
            return Integer.parseInt(env.get(TXES_PROPERTY));
        } else {
            return txes;
        }
    }

    public static int getWarmupTxes(int txes) {
        var env = System.getenv();
        if (env.containsKey(WARMUP_PROPERTY)) {
            return Integer.parseInt(env.get(WARMUP_PROPERTY));
        } else {
            return txes;
        }
    }
}
