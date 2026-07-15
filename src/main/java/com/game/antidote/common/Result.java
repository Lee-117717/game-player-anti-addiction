package com.game.antidote.common;

import lombok.Data;

/**
 * 统一API返回封装类
 * 所有Controller接口统一使用此类包裹返回数据
 *
 * @param <T> 数据类型
 */
@Data
public class Result<T> {

    /** 状态码: 200-成功, 500-失败 */
    private int code;

    /** 提示信息 */
    private String message;

    /** 数据体 */
    private T data;

    /** 时间戳 */
    private long timestamp;

    private Result() {}

    // ==================== 静态工厂方法 ====================

    public static <T> Result<T> ok(T data) {
        Result<T> r = new Result<>();
        r.code = 200;
        r.message = "success";
        r.data = data;
        r.timestamp = System.currentTimeMillis();
        return r;
    }

    public static <T> Result<T> ok(T data, String message) {
        Result<T> r = ok(data);
        r.message = message;
        return r;
    }

    public static <T> Result<T> fail(String message) {
        Result<T> r = new Result<>();
        r.code = 500;
        r.message = message;
        r.data = null;
        r.timestamp = System.currentTimeMillis();
        return r;
    }

    public static <T> Result<T> fail(int code, String message) {
        Result<T> r = new Result<>();
        r.code = code;
        r.message = message;
        r.data = null;
        r.timestamp = System.currentTimeMillis();
        return r;
    }
}
