package shopcart;

/**
 * twitter的snowflake算法 -- java实现
 * 
 * @author beyond
 * @date 2016/11/26
 */
public class SnowFlake {

    /**
     * 起始的时间戳
     */
    private final static long START_STMP = 1480166465631L;

    /**
     * 每一部分占用的位数
     */
    private final static long SEQUENCE_BIT = 12; //序列号占用的位数12
    private final static long MACHINE_BIT = 5;   //机器标识占用的位数 
    private final static long DATACENTER_BIT = 5;//数据中心占用的位数

    /**
     * 每一部分的最大值
     */
    private final static long MAX_DATACENTER_NUM = -1L ^ (-1L << DATACENTER_BIT); //31
    private final static long MAX_MACHINE_NUM = -1L ^ (-1L << MACHINE_BIT); //支持31台机器
    private final static long MAX_SEQUENCE = -1L ^ (-1L << SEQUENCE_BIT); //4095

    /**
     * 每一部分向左的位移
     */
    private final static long MACHINE_LEFT = SEQUENCE_BIT; //12
    private final static long DATACENTER_LEFT = SEQUENCE_BIT + MACHINE_BIT;//17
    private final static long TIMESTMP_LEFT = DATACENTER_LEFT + DATACENTER_BIT;//22

    private long datacenterId;  //数据中心
    private long machineId;     //机器标识
    private long sequence = 0L; //序列号
    private long lastStmp = -1L;//上一次时间戳

    public SnowFlake(long datacenterId, long machineId) {
        if (datacenterId > MAX_DATACENTER_NUM || datacenterId < 0) {
            throw new IllegalArgumentException("datacenterId can't be greater than MAX_DATACENTER_NUM or less than 0");
        }
        if (machineId > MAX_MACHINE_NUM || machineId < 0) {
            throw new IllegalArgumentException("machineId can't be greater than MAX_MACHINE_NUM or less than 0");
        }
        this.datacenterId = datacenterId;
        this.machineId = machineId;
    }

    /**
     * 产生下一个ID
     *
     * @return
     */
    public synchronized long nextId() {
        long currStmp = getNewstmp();
        if (currStmp < lastStmp) {
            throw new RuntimeException("Clock moved backwards.  Refusing to generate id");
        }

        if (currStmp == lastStmp) {
            //相同毫秒内，序列号自增
            sequence = (sequence + 1) & MAX_SEQUENCE;
            //同一毫秒的序列数已经达到最大
            if (sequence == 0L) {
                currStmp = getNextMill();
            }
        } else {
            //不同毫秒内，序列号置为0
            sequence = 0L;
        }

        lastStmp = currStmp;
        
//        long time = (currStmp - START_STMP) << TIMESTMP_LEFT;
//        long datacenter = datacenterId << DATACENTER_LEFT;
//        long machine = machineId << MACHINE_LEFT;
//        long seq = sequence;
//        System.out.print(" " + "time: " + time + "  datacenter: " + datacenter + " machine: " + machine + " sequence: " + seq + "  ");
//        System.out.print(" machine: "+machine +" " + " datacenternum: " + datacenter + " ");
        
        return (currStmp - START_STMP) << TIMESTMP_LEFT //时间戳部分
                | datacenterId << DATACENTER_LEFT       //数据中心部分
                | machineId << MACHINE_LEFT             //机器标识部分
                | sequence;                             //序列号部分
    }

    private long getNextMill() {
        long mill = getNewstmp();
        while (mill <= lastStmp) {
            mill = getNewstmp();
        }
        return mill;
    }

    private long getNewstmp() {
        return System.currentTimeMillis();
    }
    
//    public static void main(String[] args) {
//        SnowFlake snowFlake = new SnowFlake(2, 3);
//        Jedis jedis = new Jedis("localhost");
//        jedis.lpush("id_List", ""+snowFlake.nextId());
//        
//
//        for (int i = 0; i < (1 << 12); i++) {
//            System.out.println(snowFlake.nextId() + " " + i);
//            
//            //1<<10 0-1023
//            //1<<11 0-2047
//            //1<<12 0-4095
//            
//            long id = snowFlake.nextId();
//            long dbnum = (id / 10) % 8 + 1;
//            long tbnum =  id % 10;
//            jedis.lpush("id_list", "" + dbnum + "" + tbnum + "" + snowFlake.nextId());
//            
//        }
//        
//        List<String> list = jedis.lrange("id_list", 0, 2000);
//        for(int i = 0; i < list.size(); i++) {
//        	System.out.println(list.get(i) + " " + i);
//        }
//    }
}

