package cn.evun.test.simple.l2;

public abstract class Base<T,K> {
    @Provider(type = Processor.class)
    abstract public void add(T p);
}
