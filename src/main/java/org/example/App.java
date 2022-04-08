package org.example;

import java.util.Timer;
import java.util.TimerTask;

import static org.example.QueueTP.*;


/**
 * Exercice Cloud/infra file d'attente
 */
public class App {
	// please enter the connexion string of your storage instead of the empty string below for the connectStr variable 
    public static String connectStr ="";
    public static String queue = createQueue(connectStr);

    private static class Producteur extends TimerTask {
        @Override
        public void run() {
            addQueueMessage(App.connectStr, App.queue, "a new message appears!");

        }
    }
    private static class Consommateur extends TimerTask {

        @Override
        public void run() {
            dequeueMessage(App.connectStr, App.queue);
        }
    }


    public static void goProgram() {
        Timer timer;
        timer = new Timer();
        timer.schedule(new Producteur(), 0, 1000);
        timer.schedule(new Consommateur(), 10000, 20000);
        timer.schedule(new Consommateur(), 20000, 20000);

    }


    public static void main(String[] args) {
        System.out.println("Starting...");
        goProgram();


    }
}
