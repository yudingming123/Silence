package com.tm.orm.starter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @Author yudm
 * @Date 2021/6/1 14:22
 * @Desc
 */
@ConfigurationProperties(prefix = "silence")
public class SilenceProperty {
    private boolean enable;

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }
}
