package cn.auth;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

public class Redis extends Jedis {
	
	Jedis redis = new Jedis("127.0.0.1", 6379);
	JedisPoolConfig poolConfig = new JedisPoolConfig();
	

    public String get(String key) {

            return redis.get("name");
    }

    public String set(String key, String value) {

            return redis.set(key, value);
    }

    public Long del(String... keys) {

            return redis.del(keys);
    }

    // ¼üÖµÔö¼Ó×Ö·û

    public Long append(String key, String str) {

            return redis.append(key, str);
    }

    public Boolean exists(String key) {

            return redis.exists(key);
    }

    // Need research

    public Long setnx(String key, String value) {

            return redis.setnx(key, value);
    }

    public String setex(String key, String value, int seconds) {

            return redis.setex(key, seconds, value);
    }

    public Long setrange(String key, String str, int offset) {

            return redis.setrange(key, offset, str);
    }

    public List<String> mget(String... keys) {

            return redis.mget(keys);
    }

    public String mset(String... keys) {

            return redis.mset(keys);
    }

    public Long msetnx(String... keysvalues) {

            return redis.msetnx(keysvalues);
    }

    public String getset(String key, String value) {

            return redis.getSet(key, value);
    }

    public String hmset(String key, Map<String, String> hash) {

            return redis.hmset(key, hash);
    }

    public Map<String, String> hgetall(String key) {

            return redis.hgetAll(key);
    }

    public String hget(final String key, final String field) {

            return redis.hget(key, field);
    }

    public Long hset(final String key, final String field, final String value) {

            return redis.hset(key, field, value);
    }

    public Long expire(final String key, final int seconds) {

            return redis.expire(key, seconds);
    }

    public Boolean hexists(final String key, final String field) {

            return redis.hexists(key, field);
    }
    
    
    public Set<HostAndPort> PoolInitial() {
	    
	    Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();
	    
	    nodes.add(new HostAndPort("127.0.0.1", 7000));

	    nodes.add(new HostAndPort("127.0.0.1", 7001));

	    nodes.add(new HostAndPort("127.0.0.1", 7002));

	    nodes.add(new HostAndPort("127.0.0.1", 7003));

	    nodes.add(new HostAndPort("127.0.0.1", 7004));

	    nodes.add(new HostAndPort("127.0.0.1", 7005));
	    
	    return nodes;
    }    
}