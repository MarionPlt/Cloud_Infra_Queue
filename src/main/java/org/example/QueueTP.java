package org.example;

import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.models.QueueMessageItem;
import com.azure.storage.queue.models.QueueProperties;
import com.azure.storage.queue.models.QueueStorageException;

import java.util.Random;

public class QueueTP {


    public static String createQueue(String connectStr) {
        try {
            // Create a unique name for the queue
            String queueName = "queue-" + java.util.UUID.randomUUID();

            System.out.println("Creating queue: " + queueName);

            // Instantiate a QueueClient which will be
            // used to create and manipulate the queue
            QueueClient queue = new QueueClientBuilder()
                    .connectionString(connectStr)
                    .queueName(queueName)
                    .buildClient();

            // Create the queue
            queue.create();
            return queue.getQueueName();
        } catch (QueueStorageException e) {
            // Output the exception message and stack trace
            System.out.println("Error code: " + e.getErrorCode() + "Message: " + e.getMessage());
            return null;
        }
    }

    public static void addQueueMessage
            (String connectStr, String queueName, String messageText) {
        try {
            // Instantiate a QueueClient which will be
            // used to create and manipulate the queue
            QueueClient queueClient = new QueueClientBuilder()
                    .connectionString(connectStr)
                    .queueName(queueName)
                    .buildClient();

            System.out.println("Adding message to the queue: " + messageText);

            // Add a message to the queue
            queueClient.sendMessage(messageText);
        } catch (QueueStorageException e) {
            // Output the exception message and stack trace
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }


    public static void dequeueMessage(String connectStr, String queueName) {
        try {
            // Instantiate a QueueClient which will be
            // used to create and manipulate the queue
            QueueClient queueClient = new QueueClientBuilder()
                    .connectionString(connectStr)
                    .queueName(queueName)
                    .buildClient();

            while (getQueueLength(connectStr, queueName) > 0) {

                // Get the first queue message
                QueueMessageItem message = queueClient.receiveMessage();
                if (null != message) {
                    //10% failure
                    Random r = new Random();
                    int n = r.nextInt(10);

                    if (n != 1) {
                        System.out.println("Dequeing message: " + message.getMessageText());

                        // Delete the message
                        queueClient.deleteMessage(message.getMessageId(), message.getPopReceipt());
                    }else{
                        System.out.println("10% failure, message untreated");

                    }
                } else {
                    System.out.println("No message");
                }
            }
        } catch (QueueStorageException e) {
            // Output the exception message and stack trace
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public static long getQueueLength(String connectStr, String queueName) {
        long messageCount = -1;
        try {
            // Instantiate a QueueClient which will be
            // used to create and manipulate the queue
            QueueClient queueClient = new QueueClientBuilder()
                    .connectionString(connectStr)
                    .queueName(queueName)
                    .buildClient();

            QueueProperties properties = queueClient.getProperties();
            messageCount = properties.getApproximateMessagesCount();

            System.out.printf("Queue length: %d%n", messageCount);
        } catch (QueueStorageException e) {
            // Output the exception message and stack trace
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        return messageCount;
    }
}

