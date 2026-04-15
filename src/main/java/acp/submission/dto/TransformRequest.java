package acp.submission.dto;

public class TransformRequest {
    private String readQueue;
    private String writeQueue;
    private int messageCount;

    public String getReadQueue() { return readQueue; }
    public String getWriteQueue() { return writeQueue; }
    public int getMessageCount() { return messageCount; }
}