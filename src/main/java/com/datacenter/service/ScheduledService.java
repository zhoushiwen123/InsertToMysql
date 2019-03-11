package com.datacenter.service;

import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson.JSON;
import com.datacenter.entity.Constant;
import com.datacenter.entity.KafkaConsumerEntity;
import com.datacenter.utils.PropertiesConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;

/**
 * 定时处理批量入库
 */
@Component
public class ScheduledService {

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    PropertiesConfig propertiesConfig;
    private Map<String/**旧表名*/, String /**新表名*/> tableMatchOldAndNew;


    @Value("${insert.to.tidb.batch:5}")
    private Integer  batch;

    private static int corePoolSize = 5;

    private static ThreadPoolExecutor executorPutMap = new ThreadPoolExecutor(corePoolSize, corePoolSize * 2, 10l, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    private static ThreadPoolExecutor executorInsertTidb = new ThreadPoolExecutor(corePoolSize, corePoolSize * 2, 10l, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledService.class);

    public Integer getBatch() {
        return batch;
    }

    public void setBatch(Integer batch) {
        this.batch = batch;
    }

    /**
     * 任务池
     */
    private static Map<String /** 表名 */, HandlerDataMysql> handlerMap = new ConcurrentHashMap();

    /**
     * 表名与Mapper对象的映射
     */
    @PostConstruct
    public void init() {
        tableMatchOldAndNew = propertiesConfig.getTableMatchOldAndNew();
    }

    /**
     * 1S执行一次  入库操作
     */
    @Scheduled(cron = "0/1 * * * * *")
    public void scheduledExecutor() {
//        LOG.info( "scheduledExecutor execute start:" );
        try {
            //取出map数据
            Collection<HandlerDataMysql> cons = handlerMap.values();
            if(cons.isEmpty()){
                LOG.info("list is empty....");
                return ;
            }
            for (HandlerDataMysql handlerDataMysql : cons) {
                try {
                    int count = handlerDataMysql.orgs.size()/batch;
                    if(count>0){
                        //旧数据入库
                        executorInsertTidb.execute(new Runnable() {
                            @Override
                            public void run() {
                                int[]  counts = null;
                                List list = null;
                                long start = System.currentTimeMillis();
                                for(int i = 0;i<count;i++){
                                    list = new ArrayList();
                                    for(int j = 0;j<batch;j++){
                                        try {
                                            LOG.info(  "take start .....");
                                            /**
                                             * 最后一次循环可能不够batch条
                                             */
                                            if(handlerDataMysql.orgs.size()>0){
                                                list.add(handlerDataMysql.orgs.take());
                                            }
                                            LOG.info(  "take end .....");
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                    try{
                                        counts = jdbcTemplate.batchUpdate(handlerDataMysql.sql, list);
                                        LOG.info(  "execute 耗时："+(System.currentTimeMillis()-start)+"ms ;counts:"+Arrays.toString(counts) + handlerDataMysql.sql.substring(0,100));
                                        start = System.currentTimeMillis();
                                    }catch (Exception e){
                                        String msg = e.getMessage();
                                        LOG.error(handlerDataMysql.sql + " execute error:" + e.getMessage());
                                    }

                                }
                            }
                        });
                    }
                } catch (Exception e) {
                    String msg = e.getMessage();
                    LOG.error(handlerDataMysql.sql + " execute error:" + e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }



    /**
     * 解析kafka数据并入库操作
     *
     * @param content
     */
    public void kafkaToTidb(String content) {
        if (StringUtils.isEmpty(content)) {
            LOG.info(">>>>>>>> KafkaConsumerListener --> kafkaToTidb-->content is null");
            return;
        }
        /**
         * 提交任务
         */
        executorPutMap.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    KafkaConsumerEntity kafkaConsumerEntity = JSON.parseObject(content, KafkaConsumerEntity.class);
                    String key = kafkaConsumerEntity.getTable() + kafkaConsumerEntity.getType();
                    HandlerDataMysql handlerDataMysql = handlerMap.get(key);
                    if (handlerDataMysql == null) {
                        handlerDataMysql = new HandlerDataMysql();
                        handlerMap.put(key, handlerDataMysql);
                    }
                    handlerDataMysql.jdbcInsert(kafkaConsumerEntity);
                } catch (Exception e) {
                    LOG.info(">>>>>>>> KafkaConsumerListener --> kafkaToTidb-->put to map error:" + e.getMessage());
                }

            }
        });


    }


    /**
     * 插入到数据库
     */
    class HandlerDataMysql {
        /**
         * 需要执行的SQL
         */
        private String sql;

        /**
         * 需要执行的SQL
         */
        private String tableName;

        /**
         * 执行的参数列表
         */
        private ArrayBlockingQueue<Object[]> orgs;

        public HandlerDataMysql() {
            orgs = new ArrayBlockingQueue(1000);
        }

        public void jdbcInsert(KafkaConsumerEntity kafkaConsumerEntity) {
            try {
                String jsonData = kafkaConsumerEntity.getData();
                kafkaConsumerEntity.setTable(kafkaConsumerEntity.getTable().toUpperCase());
                String type = kafkaConsumerEntity.getType();
                String tableName = tableMatchOldAndNew.get(kafkaConsumerEntity.getTable());
                if (StringUtils.isEmpty(tableName)) {
                    LOG.error("未知的表 !" + kafkaConsumerEntity.getTable());
                    return;
                }
                if (StringUtils.equalsIgnoreCase(type, Constant.OprateType.INSERT)) {
                    bringInsertSql(jsonData, tableName);
                } else if (StringUtils.equalsIgnoreCase(type, Constant.OprateType.UPDATE)) {
                    bringIUpdateSql(jsonData, tableName);
                } else {
                    //其他类型
                }
            } catch (Exception e) {
                LOG.error(sql + " parse error !");
            }

        }

        /**
         * @param jsonData
         * @param tableName
         */
        private void bringIUpdateSql(String jsonData, String tableName) {
            Map<String, Object> jsonMap = JSON.parseObject(jsonData, Map.class);
            //update `ODS`.`ODS_TFR_UNLOAD_WAYBILL_DETAIL` set `TRANSPORT_TYPE`='精准卡航2', `PACK`='1纸2' where `ID`='123d89d0d59-a8b0-4
            Object arg[] = null;
            /**
             * 配置主键的名称，默认是ID
             */
            String primaryKey = "ID";
            StringBuffer updateSql = new StringBuffer("update  " + tableName + " set ");

            /**
             * 组装条件SQL
             * 如果包含主键  就用主键为条件
             * 不包含主键暂时没法处理
             */
            StringBuffer condSql = new StringBuffer(" where ");
            /**
             * 找到ID了就不再判断
             */
            boolean flag = true;
            int size = jsonMap.size();
            arg = new Object[size];
            int i = 0;
            for (String key : jsonMap.keySet()) {
                if (flag && StringUtils.equalsIgnoreCase(key, primaryKey)) {
                    condSql.append(primaryKey + " = ?");
                    arg[arg.length - 1] = jsonMap.get(primaryKey);
                    flag = false;
                } else {
                    updateSql.append(key + "= ?,");
                    arg[i++] = jsonMap.get(key);
                }
            }
            updateSql.deleteCharAt(updateSql.length() - 1);
            this.sql = updateSql.toString() + condSql;
            try {
                this.orgs.put(arg);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        /**
         * 根据json生成SQL
         *
         * @param jsonData
         * @return
         */
        private void bringInsertSql(String jsonData, String tableName) {
            StringBuffer insertSql = new StringBuffer("insert into " + tableName + " (");
            StringBuffer insertSqlEnd = new StringBuffer(") values  (");
            String sql = "";
            Map<String, Object> jsonMap = JSON.parseObject(jsonData, Map.class);
            Object arg[] = new Object[jsonMap.keySet().size()];
            int i = 0;
            for (String key : jsonMap.keySet()) {
                insertSql.append(key + ",");
                insertSqlEnd.append("? ,");
                //测试用的随机ID
                if (StringUtils.equalsIgnoreCase(key, "ID")) {
                    arg[i++] = Math.random();
                } else {
                    arg[i++] = jsonMap.get(key);
                }
            }
            //删除最后的逗号
            insertSqlEnd.deleteCharAt(insertSqlEnd.length() - 1);
            insertSql.deleteCharAt(insertSql.length() - 1);
            this.sql = insertSql.toString() + insertSqlEnd + ")";
            try {
                this.orgs.put(arg);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }
}





