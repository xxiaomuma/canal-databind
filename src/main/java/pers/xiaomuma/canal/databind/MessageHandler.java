package pers.xiaomuma.canal.databind;



@FunctionalInterface
public interface MessageHandler<T> {

    void handleMessage(T message);
}
