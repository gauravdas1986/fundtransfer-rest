package com.grainite.fundtransfer;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.grainite.api.Event;
import com.grainite.api.Grain;
import com.grainite.api.Grainite;
import com.grainite.api.GrainiteClient;
import com.grainite.api.GrainiteException;
import com.grainite.api.RecordMetadata;
import com.grainite.api.Table;
import com.grainite.api.Topic;
import com.grainite.api.Value;
import com.grainite.fundtransfer.bo.FundTransferEvent;
import com.jsoniter.output.JsonStream;

@RestController
@SpringBootApplication
public class FundtransferRestApplication {

	public static Random random = new Random();
	public static int numAccounts = 100;

	public static void main(String[] args) {
		SpringApplication.run(FundtransferRestApplication.class, args);
	}

	@Bean
	public CommandLineRunner CommandLineRunnerBean() {
		return (args) -> {
			System.out.println("creating and initializing account ");
			Grainite client = GrainiteClient.getClient(Constants.HOST, Constants.PORT);
			Table accountTable = client.getTable(Constants.APP_NAME, Constants.ACCOUNT_TABLE);
			for (int i = 0; i < 100; i++) {
				Grain account = accountTable.getGrain(Value.of("" + i), true);
				HashMap<String, Integer> accountValue = new HashMap<>();
				accountValue.put("balance", random.nextInt(100));
				accountValue.put("pending", 0);
				account.setValue(Value.of(JsonStream.serialize(accountValue)));
			}
			client.close();
		};
	}
	
	//read and update balance

	@CrossOrigin(origins = "*")
	@RequestMapping(value = "/postevent", method = RequestMethod.POST)
	public ResponseEntity<String> postEvent(@RequestBody String payload, @RequestParam("key") String key) {
		Grainite client = null;
		System.out.println("Working " + key + " payload " + payload);
		try {
			client = GrainiteClient.getClient(Constants.HOST, Constants.PORT);
			Topic topicObj = client.getTopic(Constants.APP_NAME, Constants.FUND_TRANSFER_REQUESTS_TOPIC);
			Event event = new Event(Value.of(key), Value.of(payload));
			RecordMetadata meta = topicObj.append(event);
			return new ResponseEntity<String>(meta.toString(), HttpStatus.OK);
		} catch (GrainiteException e1) {
			e1.printStackTrace();
			return new ResponseEntity<String>("Error", HttpStatus.INTERNAL_SERVER_ERROR);
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	@CrossOrigin(origins = "*")
	@RequestMapping(value = "/sendFundevent", method = RequestMethod.POST)
	public ResponseEntity<RecordMetadata> sendFundTransferEvents(@RequestBody FundTransferEvent event) throws GrainiteException {
		Grainite client = GrainiteClient.getClient(Constants.HOST, Constants.PORT);
		// Get topic fund_transfer_requests_topic.
		Topic topic = client.getTopic(Constants.APP_NAME, Constants.FUND_TRANSFER_REQUESTS_TOPIC);
		long currTime = System.nanoTime();
		Map<String, Object> fundTransferEventPayload = new HashMap<>();
		fundTransferEventPayload.put("msgId", "" + event.msgId);
		String debitAccount = event.debitAccount;
		String creditAccount = event.creditAccount;
		fundTransferEventPayload.put("debitAccount", "" + debitAccount);
		fundTransferEventPayload.put("creditAccount", "" + creditAccount);
		fundTransferEventPayload.put("amount", event.amount);

		fundTransferEventPayload.put("desc", event.desc);
		Event topicEvent = new Event(Value.of(debitAccount + creditAccount),
				Value.of(JsonStream.serialize(fundTransferEventPayload)));
		//Need to explore RecordMetadata
		RecordMetadata metadata = topic.append(topicEvent);
		client.close();
		
		return new ResponseEntity<RecordMetadata>(metadata, HttpStatus.OK);
//		Table table = client.getTable(Constants.APP_NAME, Constants.ACCOUNT_TABLE);
//		System.out.println("table grainid value "+table.toString()+table.getGrainAsync(Value.of(grainId)).get().getValue().toString());
//		Grain grain = table.getGrain(Value.of(grainId));
//		System.out.println("grainid value "+grain.getValue().asString()+"  "+grain.getValue().toString()+" "+grain.getKey().toString());
//		return grain.getValue().asString();
	}

	@GetMapping("/health")
	public String check() {
		System.out.println("Working");
		return "healthy";
	}

	@CrossOrigin(origins = "*")
	@GetMapping("/findgrain")
	public String findGrain(@RequestParam String[] grainIds) throws GrainiteException, InterruptedException, ExecutionException {
		Grainite client = GrainiteClient.getClient(Constants.HOST, Constants.PORT);
		Table table = client.getTable(Constants.APP_NAME, Constants.ACCOUNT_TABLE);
		StringBuffer grains = new StringBuffer();
		for (String grainId : grainIds) {
			Grain grain = table.getGrain(Value.of(grainId));
			System.out.println("table grainid value "+table.toString()+table.getGrainAsync(Value.of(grainId)).get().getValue().toString());
			System.out.println("grainid value "+grain.getValue().asString()+"  "+grain.getValue().toString()+" "+grain.getKey().toString());
			grains.append(grain.getValue().asString()+"\n");
		}
		return grains.toString();
	}

}
