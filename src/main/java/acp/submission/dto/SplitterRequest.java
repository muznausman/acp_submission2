package acp.submission.dto;

public class SplitterRequest {
    private String readQueue;
    private String writeTopicOdd;
    private String redisHashOdd;
    private String writeTopicEven;
    private String redisHashEven;
    private int messageCount;

    public String getReadQueue() { return readQueue; }
    public String getWriteTopicOdd() { return writeTopicOdd; }
    public String getRedisHashOdd() { return redisHashOdd; }
    public String getWriteTopicEven() { return writeTopicEven; }
    public String getRedisHashEven() { return redisHashEven; }
    public int getMessageCount() { return messageCount; }
}