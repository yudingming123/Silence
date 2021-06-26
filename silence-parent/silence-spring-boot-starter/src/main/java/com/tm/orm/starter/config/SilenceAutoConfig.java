package com.tm.orm.starter.config;


import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @Author yudm
 * @Date 2021/1/27 11:25
 * @Desc
 */
@Configuration
@EnableConfigurationProperties(SilenceProperty.class)
@ComponentScan({"com.tm.orm.silence.core"})
public class SilenceAutoConfig {

}
