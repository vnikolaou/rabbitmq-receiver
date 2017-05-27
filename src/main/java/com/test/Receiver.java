package com.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class Receiver {
	// standalone: 3.1 secs / 1000 messages
	// cluster (2 nodes): 5.6 / 1000 messages
	// cluster (2 nodes with reverse deployment): ?? / 1000 messages
	// remote client (no cluster): 4.5 / 1000 messages
	// cluster HA (2 nodes): ?? / 1000 messages
	// cluster HA (2 nodes with reverse deployment): ?? / 1000 messages
    @RabbitListener(bindings={@QueueBinding(value=@Queue(value="fetch-listings-page-queue", durable="true"),
        	exchange=@Exchange(value="ebay-exchange", durable="true"), 
        	key="fetch-listings-queue")})
    public void consume(Message msg) {
    	try {
    		String correlationId = getCorrelationId(msg);

	    	Map<String, Object> params = this.decodeMessageBody(msg);
	    	String input = (String)params.get("message");
	    	
	    	System.out.println("Message received: " + input + " (correliationID: " + correlationId + ")");
	    	System.out.println("Received at: " + System.currentTimeMillis());
    	} catch(Exception ex) { 
    		ex.printStackTrace();
    	}	
    }	 
    
    @SuppressWarnings("unchecked")
    private Map<String, Object> decodeMessageBody(Message msg) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(msg.getBody());
        ObjectInputStream in = new ObjectInputStream(byteIn);
        return (Map<String, Object>)in.readObject();    	
    }
    
    private String getCorrelationId(Message msg) {
    	return new String(msg.getMessageProperties().getCorrelationId());
    }
 
}
