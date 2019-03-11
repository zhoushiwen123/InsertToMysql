package com.datacenter.entity;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.Date;

/**
 * kafka消费数据转化实体类
 */
public class KafkaConsumerEntity {

    private String data;

    private String old;
    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private Date current_ts;
    private String type;

    private String table;
    private String pos;
    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private Date op_ts;

    public String getOld() {
        return old;
    }

    public void setOld(String old) {
        this.old = old;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getPos() {
        return pos;
    }

    public void setPos(String pos) {
        this.pos = pos;
    }

    public Date getOp_ts() {
        return op_ts;
    }

    public void setOp_ts(Date op_ts) {
        this.op_ts = op_ts;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public Date getCurrent_ts() {
        return current_ts;
    }

    public void setCurrent_ts(Date current_ts) {
        this.current_ts = current_ts;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "KafkaConsumerEntity{" +
                "current_ts=" + current_ts +
                ", type='" + type + '\'' +
                ", table='" + table + '\'' +
                ", pos='" + pos + '\'' +
                ", op_ts=" + op_ts +
                '}';
    }
}
