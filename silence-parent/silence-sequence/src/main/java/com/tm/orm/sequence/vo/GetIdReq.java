package com.tm.orm.sequence.vo;

/**
 * @Author yudm
 * @Date 2021/6/5 14:39
 * @Desc 获取id的请求体
 */
public class GetIdReq {
    private String db;
    private String table;
    private Integer size;

    public GetIdReq(String db, String table, Integer size) {
        this.db = db;
        this.table = table;
        this.size = size;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }
}
