package pers.xiaomuma.canal.databind.factory;

import pers.xiaomuma.canal.databind.EntryHandler;
import pers.xiaomuma.canal.databind.exception.CanalBizException;
import java.util.Set;


public interface IColumnFactory<T> {

    <R> R newInstance(EntryHandler<R> handler, T target) throws CanalBizException;

    <R> R newInstance(EntryHandler<R> handler, T target, Set<String> updateColumn) throws CanalBizException;

    <R> R newInstance(Class<R> clazz, T target) throws CanalBizException;
}
