package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaTransactionListener {

    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;

    public KafkaTransactionListener(DatabaseConduit databaseConduit,
                                    RestTemplate restTemplate) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-core"
    )
    public void listen(Transaction transaction) {

        UserRecord sender = databaseConduit.findUser(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findUser(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        Incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive",
                transaction,
                Incentive.class
        );

        float incentiveAmount = 0;

        if (incentive != null) {
            incentiveAmount = incentive.getAmount();
        }

        sender.setBalance(sender.getBalance() - transaction.getAmount());

        recipient.setBalance(
                recipient.getBalance()
                        + transaction.getAmount()
                        + incentiveAmount
        );

        databaseConduit.save(sender);
        databaseConduit.save(recipient);

        databaseConduit.saveTransaction(
                new TransactionRecord(
                        sender,
                        recipient,
                        transaction.getAmount(),
                        incentiveAmount
                )
        );

        // Temporary debug for Task 4
        UserRecord wilbur = databaseConduit.findUser(2);
        if (wilbur != null) {
            System.out.println("WILBUR = " + wilbur.getBalance());
        }
    }
}