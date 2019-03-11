package com.datacenter.utils;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.Map;

/**
 * 配置注入
 */
@Configuration
@PropertySource("classpath:application-match.properties")
@ConfigurationProperties(prefix = "base.config")
public class PropertiesConfig {

    /**
     * 新表与老表对应的映射
     */
    private Map tableMatchOldAndNew;

    private int batch ;

    public int getBatch() {
        return batch;
    }

    public void setBatch(int batch) {
        this.batch = batch;
    }

    public Map getTableMatchOldAndNew() {
        return tableMatchOldAndNew;
    }
    public void setTableMatchOldAndNew(Map tableMatchOldAndNew) {
        this.tableMatchOldAndNew = tableMatchOldAndNew;
    }
}
