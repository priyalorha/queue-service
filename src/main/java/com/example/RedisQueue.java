package com.example;

import redis.clients.jedis.Jedis;

public class RedisQueue {


    private static Jedis jedis;
    private static int count =0;

    // Initialize the Redis connection in a static block
    static {
        jedis = new Jedis("us1-alive-wildcat-41210.upstash.io", 41210, true);
        System.out.println( jedis.auth("password"));
    }




    private static void pushToQueue( String element) {
        if(count>=0) {
        jedis.set(String.valueOf(count), element);
        count++;}
    }

    private static String popFromQueue() {
        String s = null;
        if(count>=0) {
             s= jedis.get(String.valueOf(count));
            count--;
           
        }
        return s;
    }

    public static void main(String[]args){

        pushToQueue("Element 1");
        pushToQueue("Element 2");
        pushToQueue("Element 3");

        // Dequeue elements from the queue
        String element1 = popFromQueue();
        String element2 = popFromQueue();
        String element3 = popFromQueue();

        // Print dequeued elements
        System.out.println(element1); // Output: Element 1
        System.out.println(element2); // Output: Element 2
        System.out.println(element3); // Output: Element 3

    }
}
