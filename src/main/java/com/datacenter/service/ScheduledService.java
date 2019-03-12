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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 定时处理批量入库
 */
@Component
public class ScheduledService {


    @Autowired
    PropertiesConfig propertiesConfig;

    private Map<String/**旧表名*/, String /**新表名*/> tableMatchOldAndNew;


    @Value("${insert.to.tidb.batch:5}")
    private Integer batch;
    @Value("${insert.to.tidb.corePoolSize:5}")
    private  Integer corePoolSize;
    @Value("${insert.to.tidb.poolCapacity:5}")
    private Integer poolCapacity;

    private  static ThreadPoolExecutor executorPutMap ;
    private  static ThreadPoolExecutor executorInsertTidb ;
    private static final Logger LOG = LoggerFactory.getLogger(ScheduledService.class);

    /**
     * 任务池
     */
    private static Map<String /** 表名 */, HandlerDataToDb> handlerMap = new ConcurrentHashMap();

    /**
     * 表名与Mapper对象的映射
     */
    @PostConstruct
    public void init() {
        tableMatchOldAndNew = propertiesConfig.getTableMatchOldAndNew();
        executorPutMap = new ThreadPoolExecutor(corePoolSize, corePoolSize * 2, 10l, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(poolCapacity), new ThreadPoolExecutor.CallerRunsPolicy());

        executorInsertTidb = new ThreadPoolExecutor(corePoolSize, corePoolSize * 2, 10l, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(poolCapacity), new ThreadPoolExecutor.CallerRunsPolicy());


    }

    /**
     * 1S执行一次  入库操作
     */
    @Scheduled(cron = "0/1 * * * * *")
    public void scheduledExecutor() {
//        LOG.info( "scheduledExecutor execute start:" );
        try {
            //取出map数据
            Collection<HandlerDataToDb> cons = handlerMap.values();
            if (cons.isEmpty()) {
                LOG.info("list is empty....");
                return;
            }
            for (HandlerDataToDb handlerDataToDb : cons) {
                try {
                    int count = handlerDataToDb.orgs.size() / batch;
                    if (count > 0) {
                        List list = null;
                        for (int i = 0; i < count; i++) {
                            list = new ArrayList();
                            for (int j = 0; j < batch; j++) {
                                try {
                                    LOG.info("take start .....");
                                    /**
                                     * 最后一次循环可能不够batch条
                                     */
                                    if (handlerDataToDb.orgs.size() > 0) {
                                        list.add(handlerDataToDb.orgs.take());
                                    }
                                    LOG.info("take end .....");
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            if(!list.isEmpty()){
                                executorInsertTidb.execute(new ToTidbService(handlerDataToDb.sql,list));
                            }else{
                                LOG.info("list isEmpty  .....");
                            }

                        }
                    }
                } catch (Exception e) {
                    String msg = e.getMessage();
                    LOG.error(handlerDataToDb.sql + " execute error:" + e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }


    /**
     * 解析卡夫卡数据并入库操作
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
                    HandlerDataToDb handlerDataToDb = handlerMap.get(key);
                    if (handlerDataToDb == null) {
                        handlerDataToDb = new HandlerDataToDb();
                        handlerMap.put(key, handlerDataToDb);
                    }
                    handlerDataToDb.jdbcInsert(kafkaConsumerEntity);
                } catch (Exception e) {
                    LOG.info(">>>>>>>> KafkaConsumerListener --> kafkaToTidb-->put to map error:" + e.getMessage());
                }

            }
        });


    }


    class HandlerDataToDb {
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

        public HandlerDataToDb() {
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



    public Integer getBatch() {
        return batch;
    }

    public void setBatch(Integer batch) {
        this.batch = batch;
    }

    public Integer getCorePoolSize() {
        return corePoolSize;
    }

    public void setCorePoolSize(Integer corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    public Integer getPoolCapacity() {
        return poolCapacity;
    }

    public void setPoolCapacity(Integer poolCapacity) {
        this.poolCapacity = poolCapacity;
    }
}





