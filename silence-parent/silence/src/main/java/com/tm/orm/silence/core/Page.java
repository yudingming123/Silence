package com.tm.orm.silence.core;

import java.util.List;

/**
 * @author yudm
 * @date 2021/5/14 12:30
 * @desc 分页的信息类
 */
public class Page<T> {
    //是否查询数量
    private boolean searchTotal;
    //总数
    private int total;
    //页号
    private int pageNum;
    //每页大小
    private int pageSize;
    //数据列表
    private List<T> list;

    public Page(int pageNum, int pageSize, boolean searchTotal) {
        this.pageNum = pageNum;
        this.pageSize = pageSize;
        this.searchTotal = searchTotal;
    }

    public boolean isSearchTotal() {
        return searchTotal;
    }

    public void setSearchTotal(boolean searchTotal) {
        this.searchTotal = searchTotal;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getPageNum() {
        return pageNum;
    }

    public void setPageNum(int pageNum) {
        this.pageNum = pageNum;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public List<T> getList() {
        return list;
    }

    public void setList(List<T> list) {
        this.list = list;
    }
}
