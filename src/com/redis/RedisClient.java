package com.redis;
import redis.clients.jedis.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by steven.gao on 2014/8/21.
 */
public class RedisClient {
    private Jedis jedis;
    private JedisPool jedisPool;
    private ShardedJedis shardedJedis;
    private ShardedJedisPool shardedJedisPool;

    public RedisClient() {

        initialPool();

        initialShardedPool();

        shardedJedis = shardedJedisPool.getResource();
        jedis = jedisPool.getResource();
    }

    private void initialPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxActive(20);
        config.setMaxIdle(5);
        config.setMaxWait(10001);
        config.setTestOnBorrow(false);
        jedisPool = new JedisPool(config, "127.0.0.1", 6379);
    }

    private void initialShardedPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxActive(20);
        config.setMaxIdle(5);
        config.setMaxWait(10001);
        config.setTestOnBorrow(false);
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        shards.add(new JedisShardInfo("127.0.0.1", 6379, "master"));

        shardedJedisPool = new ShardedJedisPool(config, shards);
    }

    public void show() {
        keyOperate();
        stringOperate();
    }

    private void keyOperate() {
        System.out.println("==================key================");

        System.out.println("清空库中所有数据：" + jedis.flushDB());

        System.out.println("判断key999键是否存在：" + shardedJedis.exists("key999"));

        System.out.println("新增key001,value001键值对：" + shardedJedis.set("key001", "value001"));

        System.out.println("判断key001是否存在：" + shardedJedis.exists("key001"));

        System.out.println("新增key002,value002键值对：" + shardedJedis.set("key002", "value002"));

        System.out.println("系统中所有键如下：");

        Set<String> keys = jedis.keys("*");

        Iterator<String> it = keys.iterator();

        while (it.hasNext()) {
            String key = it.next();
            System.out.println(key);
        }

        System.out.println("系统中删除key002：" + jedis.del("key002"));

        System.out.println("判断key002是否存在：" + shardedJedis.exists("key002"));

        System.out.println("设置key001的过期时间为5秒：" + jedis.expire("key001", 5));
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("查看key001的剩余生存时间：" + jedis.ttl("key001"));

        System.out.println("移除key001的生存时间：" + jedis.persist("key001"));

        System.out.println("查看key001的剩余生存时间：" + jedis.ttl("key001"));

        System.out.println("查看key所存储的值的类型：" + jedis.type("key001"));
    }

    private void stringOperate() {
        System.out.println("=======================String_1================");
        System.out.println("清空库中所有数据：" + jedis.flushDB());

        System.out.println("=============增==============");

        jedis.set("key001", "value001");
        jedis.set("key002", "value002");
        jedis.set("key003", "value003");

        System.out.println("已新增的3个键值对如下：");
        System.out.println(jedis.get("key001"));
        System.out.println(jedis.get("key002"));
        System.out.println(jedis.get("key003"));

        System.out.println("===============删================");
        System.out.println("删除key003键值对：" + jedis.del("key003"));
        System.out.println("获取key003键对应的值：" + jedis.get("key003"));

        System.out.println("===============改===============");
        System.out.println("直接覆盖key001原来的数据：" + jedis.set("key001", "value001-update"));
        System.out.println("获取key001对应的新值：" + jedis.get("key001"));

        System.out.println("在key002原来值后面追加：" + jedis.append("key002", "+appendString"));
        System.out.println("获取key002对应的新值：" + jedis.get("key002"));

        System.out.println("===============增，删，查（多个）");
        System.out.println("一次性新增key201,key202,key203,key204及其对于值：" + jedis.mset("key201", "value201", "key202", "value202", "key203", "value203", "key204", "value204"));

        System.out.println("一次性获取key201,key202,key203,key204各自对于的值：" + jedis.mget("hey201", "key202", "key203", "key204"));

        System.out.println("一次性删除key201,key202: " + jedis.del(new String[]{"key201", "key202"}));

        System.out.println("一次性获取key201,key202,key203,key204各自对于的值：" + jedis.mget("key201", "key202", "key203", "key204"));

        System.out.println();

        System.out.println("====================String_2==================");
        System.out.println("清空库中所有数据：" + jedis.flushDB());

        System.out.println("================新增键值对时防止覆盖原先值===============");
        System.out.println("原先key301不存在时，新增key301：" + shardedJedis.setnx("key301", "value301"));
        System.out.println("原先key302不存在时，新增key302：" + shardedJedis.setnx("key302", "value302"));

        System.out.println("当key302存在时，尝试新增key302: " + shardedJedis.setnx("key302", "value302_new"));

        System.out.println("获取key301对应的值：" + shardedJedis.get("key301"));

        System.out.println("获取key302对应的值：" + shardedJedis.get("key302"));

        System.out.println("==============超过有效键值对被删除===============");

        System.out.println("新增key303，并指定过期时间为2秒：" + shardedJedis.setex("key303", 2, "key303-2second"));

        System.out.println("获取key303对应的值：" + shardedJedis.get("key303"));

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("3秒之后，获取key303对于的值：" + shardedJedis.get("key303"));

        System.out.println("========获取原值，更新为新值一步完成================");

        System.out.println("key302原值：" + shardedJedis.getSet("key302", "value302-after-getset"));

        System.out.println("key302新值：" + shardedJedis.get("key302"));

        System.out.println("==============获取子串=================");

        System.out.println("获取key302对应值中的字串：" + shardedJedis.getrange("key302", 5, 7));

    }

    public static void main(String[] args) {
        RedisClient redisClient = new RedisClient();
        redisClient.show();
    }
}
