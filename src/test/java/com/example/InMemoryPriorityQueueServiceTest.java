package com.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class InMemoryPriorityQueueServiceTest {
	private QueueService qs;
	private String queueUrl = "https://sqs.ap-1.amazonaws.com/007/MyQueue";
	
	@Before
	public void setup() {
		qs = new InMemoryPriorityQueueService();
	}
	
	
	@Test
	public void testSendMessage(){
		qs.push(queueUrl, "Good message!", 4);
		Message msg = qs.pull(queueUrl);

		assertNotNull(msg);
		assertEquals("Good message!", msg.getBody());
	}
	
	@Test
	public void testPullMessage(){
		String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null }";
		
		qs.push(queueUrl, msgBody, 4);
		Message msg = qs.pull(queueUrl);

		assertEquals(msgBody, msg.getBody());
		assertTrue(msg.getReceiptId() != null && msg.getReceiptId().length() > 0);
	}

	@Test
	public void testPullEmptyQueue(){
		Message msg = qs.pull(queueUrl);
		assertNull(msg);
	}
	
	@Test
	public void testDoublePull(){
		qs.push(queueUrl, "Message A.", 4);
		qs.pull(queueUrl);
		Message msg = qs.pull(queueUrl);
		assertNull(msg);
	}
	
	@Test
	public void testDeleteMessage(){
		String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null }";
		
		qs.push(queueUrl, msgBody, 4);
		Message msg = qs.pull(queueUrl);

		qs.delete(queueUrl, msg.getReceiptId());
		msg = qs.pull(queueUrl);
		
		assertNull(msg);
	}
	
	@Test
	public void testFIFO3Msgs(){
		String [] msgStrs = {"Test msg 0", "test msg 1",
				"Test msg 2", "Test msg 3"};
//		qs.push(queueUrl, msgStrs[0],5);
		qs.push(queueUrl, msgStrs[0],3);
		qs.push(queueUrl, msgStrs[1],5);
		qs.push(queueUrl, msgStrs[2],1);
        qs.push(queueUrl, msgStrs[3],1);

		Message msg1 = qs.pull(queueUrl);
		Message msg2 = qs.pull(queueUrl);
		Message msg3 = qs.pull(queueUrl);
        Message msg4 = qs.pull(queueUrl);
		
		org.junit.Assert.assertTrue(msgStrs[2] == msg1.getBody()
				&& msgStrs[3] == msg2.getBody() && msgStrs[0] == msg3.getBody() &&
                msgStrs[1] == msg4.getBody() );
	}
	
	@Test
	public void testAckTimeout(){
		InMemoryQueueService queueService = new InMemoryQueueService() {
			long now() {
				return System.currentTimeMillis() + 1000 * 30 + 1;
			}
		};
		
		queueService.push(queueUrl, "Message A.", 4);
		queueService.pull(queueUrl);
		Message msg = queueService.pull(queueUrl);
		assertTrue(msg != null && msg.getBody() == "Message A.");
	}
}
