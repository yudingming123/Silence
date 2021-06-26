package com.tm.orm.sequence.vo;

/**
 * @Author yudm
 * @Date 2021/6/5 14:57
 * @Desc 设置/重置id的请求体
 */
public class SetIdReq {
    private String db;
    private String table;
    private Integer id;

    public SetIdReq(String db, String table, Integer id) {
        this.db = db;
        this.table = table;
        this.id = id;
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

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }
}
