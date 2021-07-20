package com.tm.orm.test.user;

import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

/**
 * @Author yudm
 * @Date 2021/7/19 17:02
 * @Desc
 */
public class TypeReference<T> {
    protected final Type type;

    protected TypeReference() {
        Type superClass = this.getClass().getGenericSuperclass();
        this.type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
    }

    protected TypeReference(Type... actualTypeArguments) {
        Type superClass = this.getClass().getGenericSuperclass();
        ParameterizedType argType = (ParameterizedType) ((ParameterizedType) superClass).getActualTypeArguments()[0];
        Type rawType = argType.getRawType();
        Type[] argTypes = argType.getActualTypeArguments();
        int actualIndex = 0;

        for (int i = 0; i < argTypes.length; ++i) {
            if (argTypes[i] instanceof TypeVariable) {
                argTypes[i] = actualTypeArguments[actualIndex++];
                if (actualIndex >= actualTypeArguments.length) {
                    break;
                }
            }
        }

        this.type = ParameterizedTypeImpl.make(this.getClass(), argTypes, rawType);
    }

    public Type getType() {
        return this.type;
    }
}
