package shopcart;

/**
 * twitter��snowflake�㷨 -- javaʵ��
 * 
 * @author beyond
 * @date 2016/11/26
 */
public class SnowFlake {

    /**
     * ��ʼ��ʱ���
     */
    private final static long START_STMP = 1480166465631L;

    /**
     * ÿһ����ռ�õ�λ��
     */
    private final static long SEQUENCE_BIT = 12; //���к�ռ�õ�λ��12
    private final static long MACHINE_BIT = 5;   //������ʶռ�õ�λ�� 
    private final static long DATACENTER_BIT = 5;//��������ռ�õ�λ��

    /**
     * ÿһ���ֵ����ֵ
     */
    private final static long MAX_DATACENTER_NUM = -1L ^ (-1L << DATACENTER_BIT); //31
    private final static long MAX_MACHINE_NUM = -1L ^ (-1L << MACHINE_BIT); //֧��31̨����
    private final static long MAX_SEQUENCE = -1L ^ (-1L << SEQUENCE_BIT); //4095

    /**
     * ÿһ���������λ��
     */
    private final static long MACHINE_LEFT = SEQUENCE_BIT; //12
    private final static long DATACENTER_LEFT = SEQUENCE_BIT + MACHINE_BIT;//17
    private final static long TIMESTMP_LEFT = DATACENTER_LEFT + DATACENTER_BIT;//22

    private long datacenterId;  //��������
    private long machineId;     //������ʶ
    private long sequence = 0L; //���к�
    private long lastStmp = -1L;//��һ��ʱ���

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
     * ������һ��ID
     *
     * @return
     */
    public synchronized long nextId() {
        long currStmp = getNewstmp();
        if (currStmp < lastStmp) {
            throw new RuntimeException("Clock moved backwards.  Refusing to generate id");
        }

        if (currStmp == lastStmp) {
            //��ͬ�����ڣ����к�����
            sequence = (sequence + 1) & MAX_SEQUENCE;
            //ͬһ������������Ѿ��ﵽ���
            if (sequence == 0L) {
                currStmp = getNextMill();
            }
        } else {
            //��ͬ�����ڣ����к���Ϊ0
            sequence = 0L;
        }

        lastStmp = currStmp;
        
//        long time = (currStmp - START_STMP) << TIMESTMP_LEFT;
//        long datacenter = datacenterId << DATACENTER_LEFT;
//        long machine = machineId << MACHINE_LEFT;
//        long seq = sequence;
//        System.out.print(" " + "time: " + time + "  datacenter: " + datacenter + " machine: " + machine + " sequence: " + seq + "  ");
//        System.out.print(" machine: "+machine +" " + " datacenternum: " + datacenter + " ");
        
        return (currStmp - START_STMP) << TIMESTMP_LEFT //ʱ�������
                | datacenterId << DATACENTER_LEFT       //�������Ĳ���
                | machineId << MACHINE_LEFT             //������ʶ����
                | sequence;                             //���кŲ���
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

