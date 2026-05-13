package com.nnrz.util;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Properties;

/**
 * Debezium 自定义时间转换器
 * 解决官方 TimestampConverter 时区错乱、空值异常、格式不兼容问题
 */
public class CustomDateTimeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    @Override
    public void configure(Properties props) {
        // 支持从配置文件读取时间格式
        //String format = props.getProperty("datetime.format", "yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        // 只处理 datetime / timestamp 类型字段
        String typeName = column.typeName().toLowerCase();
        if(!"datetime".equals(typeName) && !"timestamp".equals(typeName)){
            return;
        }

        registration.register(SchemaBuilder.string(), value -> {
            if(value == null){
                return null;
            }

            try{
                //把时间里面的多余时区字符去掉
                return value.toString().replaceAll("[A-Z]+", " ").replaceAll("\\s+", " ").trim();

            } catch(Exception e){
                return value.toString();
            }
        });
    }
}