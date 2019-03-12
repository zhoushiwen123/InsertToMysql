package com.datacenter.service;

import com.deppon.datacenter.utils.SpringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Arrays;
import java.util.List;

public class ToTidbService implements Runnable{

    JdbcTemplate jdbcTemplate;


    private static final Logger LOG = LoggerFactory.getLogger(ToTidbService.class);

    private String sql;

    private List<Object[]> batchArgs;

    public ToTidbService(String sql, List<Object[]> batchArgs){
        this.sql = sql;
        this.batchArgs = batchArgs;
        jdbcTemplate = ((JdbcTemplate)SpringUtil.getBean("jdbcTemplate"));
    }


    @Override
    public void run() {
        int[] counts = null;
        long start = System.currentTimeMillis();
        try{
            counts = jdbcTemplate.batchUpdate(this.sql, this.batchArgs);
        }catch (Exception e) {
            LOG.error("jdbcTemplate batchUpdate sql : "+this.sql.substring(0,100)+"; error："+e.getMessage());
        }
        LOG.info("execute 耗时：" + (System.currentTimeMillis() - start) + "ms ;LIST SIZE :" + this.batchArgs.size() + " ;counts:" + Arrays.toString(counts) + this.sql.substring(0, 100));
    }
}
