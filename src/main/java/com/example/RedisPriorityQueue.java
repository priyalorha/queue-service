import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;

import java.util.Set;

public class RedisPriorityQueue {

    private final Jedis jedis;
    private final String queueName;

    public RedisPriorityQueue(String host, int port, String password, String queueName) {
        this.jedis = new Jedis(host, port);
        this.jedis.auth(password);

        this.queueName = queueName;
    }

    public void enqueue(String item, double priority) {
        jedis.zadd(queueName, priority, item);
    }

    public String dequeue() {
        Set<Tuple> items = (Set<Tuple>) jedis.zrangeWithScores(queueName, 0, 0);
        if (items.isEmpty()) {
            return null;
        }
        Tuple tuple = items.iterator().next();
        String item = tuple.getElement();
        jedis.zrem(queueName, item);
        return item;
    }

    public String peek() {
        Set<Tuple> items = (Set<Tuple>) jedis.zrangeWithScores(queueName, 0, 0);
        if (items.isEmpty()) {
            return null;
        }
        return items.iterator().next().getElement();
    }

    public long size() {
        return jedis.zcard(queueName);
    }

    public void clear() {
        jedis.del(queueName);
    }

    public static void main(String[] args) {


        RedisPriorityQueue queue = new RedisPriorityQueue("us1-alive-wildcat-41210.upstash.io", 41210, "password", "MY_QUEUE");

        // Enqueue some items with priorities
        queue.enqueue("item1", 5);
        queue.enqueue("item2", 3);
        queue.enqueue("item3", 7);

        // Peek at the top item without removing it
        System.out.println("Top item: " + queue.peek());

        // Dequeue the top item
        System.out.println("Dequeued item: " + queue.dequeue());

        // Check the size of the queue
        System.out.println("Queue size: " + queue.size());

        // Clear the queue
        queue.clear();
    }
}
