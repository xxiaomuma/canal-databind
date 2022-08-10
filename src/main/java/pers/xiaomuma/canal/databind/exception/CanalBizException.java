package pers.xiaomuma.canal.databind.exception;

public class CanalBizException extends RuntimeException {

    public CanalBizException(String msg) {
        super(msg);
    }

    public CanalBizException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
