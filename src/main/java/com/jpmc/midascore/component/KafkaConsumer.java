package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;

    public KafkaConsumer(DatabaseConduit databaseConduit, RestTemplate restTemplate) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());

        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {
            String url = "http://localhost:8080/incentive";
            Incentive incentive = restTemplate.postForObject(url, transaction, Incentive.class);
            float incentiveAmount = (incentive != null) ? incentive.getAmount() : 0f;

            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

            databaseConduit.saveUser(sender);
            databaseConduit.saveUser(recipient);
            databaseConduit.saveTransaction(new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount));
            
            logger.info("Transaction processed successfully with incentive: {}", incentiveAmount);
        } else {
            logger.warn("Transaction invalid: senderId={}, recipientId={}, amount={}", 
                transaction.getSenderId(), transaction.getRecipientId(), transaction.getAmount());
        }
    }
}
