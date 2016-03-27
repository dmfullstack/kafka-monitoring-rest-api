package com.jaeminsung.domain;

/**
 * 
 * OffsetInfo is a domain object class that holds offsets and computes
 * consumer group lag (num. of produced messages - num. of consumed messages)
 * 
 * @author jaeminsung
 * @version 1.0
 * @since 3/26/2016
 */
public class OffsetInfo {
    private final long latestOffset, committedOffset, lag;

    public OffsetInfo(long latestOffset, long committedOffset) {
        this.latestOffset = latestOffset;
        this.committedOffset = committedOffset;
        
        // Check whether the latest offset is -1L, which indicates that an error occurred
        // while obtaining the offset
        this.lag = (latestOffset != -1L) ? (latestOffset - committedOffset) : -1L;
    }

    public long getLatestOffset() {
        return latestOffset;
    }
    
    public long getCommitedOffset() {
        return committedOffset;
    }
    
    public long getLag() {
        return lag;
    }
}
