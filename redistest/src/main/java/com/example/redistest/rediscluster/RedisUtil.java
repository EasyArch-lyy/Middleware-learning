package com.example.redistest.rediscluster;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RedisUtil {

    private JedisCluster jedisCluster;

    public RedisUtil() {
        String redisString = "redisCluster=192.168.119.128:7000,192.168.119.128:7001,192.168.119.128:7002";
        String[] hostArray = redisString.split(",");
        Set<HostAndPort> nodes = new HashSet<>();
        JedisPoolConfig config = new JedisPoolConfig();
        for (String host : hostArray) {
            String[] detail = host.split(":");
            nodes.add(new HostAndPort(detail[0], Integer.parseInt(detail[1])));
        }
        jedisCluster = new JedisCluster(nodes, 3000, 3000, 10, "", config);
    }

    /**
     * 获取redis中指定key的值，value类型为String的使用此方法
     */
    public String get(String key) {

        return jedisCluster.get(key);
    }

    public String hget(String key, String fieldName) {

        return jedisCluster.hget(key, fieldName);
    }

    public Map<String, String> hgetAll(String key)
    {
        return jedisCluster.hgetAll(key);
    }

    /**
     * 设置redis中指定key的值，value类型为String的使用此方法
     */
    public void set(String key, String value)
    {
        jedisCluster.set(key,value);
    }

    /**
     * 获取redis中指定key的值,对应的value，value类型为MAP的使用此方法
     */
    public Map<String, String> getMap(String key)
    {
        return jedisCluster.hgetAll(key);
    }

    /**
     * 存储map
     * @param key
     * @param map
     * @return
     */
    public String hmset(String key, Map<String, String> map) {
        return jedisCluster.hmset(key, map);
    }

    public Long hset(String mapName, String key, String value) {
        return jedisCluster.hset(mapName, key, value);
    }

    /**
     * 删除redis中指定key的值项
     * @return
     */
    public String del(String key)
    {
        jedisCluster.del(key);
        return key;
    }

    public void close() throws IOException {
        jedisCluster.close();;
    }
}
