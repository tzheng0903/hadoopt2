package cn.evun.test.simple.l1;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class Base<T,K> {
    public K process(T t){
        Type genericSuperclass = this.getClass().getGenericSuperclass();
        ParameterizedType pt =(ParameterizedType)genericSuperclass;
        Type actualTypeArgument = pt.getActualTypeArguments()[0];
        System.out.println(actualTypeArgument);
        return null;
    }
}
