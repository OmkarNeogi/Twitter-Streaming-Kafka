import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;


public class TwitterKafkaProducer {

	public static void main(String[] args) {
		/* 
		 * Create producer and call twitter streaming function
		 * Extraction: 
		 * 		1. Collection: streamTwitter()
		 * 		2. Filter for required attributes: filterJSON()
		 * Transformation: TransfromTweets()
		 * Loading: producer.send()
		*/
		Properties properties = new Properties();
		
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		
		properties.setProperty("acks", "1");
		properties.setProperty("retries", "3");
		properties.setProperty("linger.ms", "100");
		
		Producer<String, String> producer = 
				new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);
		
		try {
			streamTwitter(producer);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
	}
	
	public static void streamTwitter(Producer<String, String> producer) throws InterruptedException {
		
		String consumerKey = "6rGVr1Oki6DUsltOGVmRrfPVd";
		String consumerSecret = "HGZt9Kzxp74hrqVlSZXzhEVYLXlCkWqNaZejlTiQwgBkkKmNel";
		String accessToken = "772927355237044224-fMFR5dK6UgsJ7SrkmzTG2asEIcrwYEc";
		String accessTokenSecret = "8Y39OEcS7yFeEbI7oAKpKeYhHIKO2aPRAZIa0RjTyPKCP";
		
		
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		List<String> terms = Lists.newArrayList("#DeathTo");
		hosebirdEndpoint.trackTerms(terms);

		Authentication hosebirdAuth = 
				new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);
		
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));
		
		Client hosebirdClient = builder.build();

		hosebirdClient.connect();
		
		while (!hosebirdClient.isDone()) {
		  String msg = msgQueue.take();
		  System.out.println(msg);
		  ProducerRecord<String, String> producerRecord = 
				  new ProducerRecord<String, String>("#Sridevi", msg);
		  producer.send(producerRecord);
		}
		
		hosebirdClient.stop();
		producer.flush();
		producer.close();
	}
}
