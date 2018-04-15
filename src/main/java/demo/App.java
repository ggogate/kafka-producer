package demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;

import com.fasterxml.jackson.databind.ObjectMapper;

import demo.model.Event;


public class App {

	public static void main(String[] args) throws Exception {

		String topicName = "test";
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
	    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
	    Producer<String, String> producer = new KafkaProducer<String, String>(props);
	    ObjectMapper mapper = new ObjectMapper();
	    Event e;
	    int i = 0;
	    while(true) {
	    	i++;
	    	e = new Event("submissions",i*10, i);
	    	producer.send(new ProducerRecord<String, String>(topicName,Integer.toString(i), mapper.writeValueAsString(e)));
	    	Thread.sleep(1000);
	    }	      
	}

}
