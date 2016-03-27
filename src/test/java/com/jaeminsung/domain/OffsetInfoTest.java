package com.jaeminsung.domain;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class OffsetInfoTest {
	
	private OffsetInfo offsetInfo;
	private long latestOffset, committedOffset;
	
	@Test
	public void testOffsetInfo() {
		latestOffset = 2L;
		committedOffset = 2L;
		offsetInfo = new OffsetInfo(latestOffset, committedOffset);
		assertEquals(offsetInfo.getLatestOffset(), 2L);
		assertEquals(offsetInfo.getCommitedOffset(), 2L);
		assertEquals(offsetInfo.getLag(), 0L);
	}
	
	@Test
	public void testOffsetInfoWithInvalidLatestOffset() {
		latestOffset = -1L;
		committedOffset = 2L;
		offsetInfo = new OffsetInfo(latestOffset, committedOffset);
		assertEquals(offsetInfo.getLatestOffset(), -1L);
		assertEquals(offsetInfo.getCommitedOffset(), 2L);
		assertEquals(offsetInfo.getLag(), -1L);
	}
}
