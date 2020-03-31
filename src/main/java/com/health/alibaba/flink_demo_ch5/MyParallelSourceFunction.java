package com.health.alibaba.flink_demo_ch5;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @Author: wjy
 * @Date: 2020/4/1 0:26
 */
public class MyParallelSourceFunction implements ParallelSourceFunction<String> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int count = 0;
        while(isRunning) {
            ctx.collect("test parallel Function addSource方式"+count);
            Thread.sleep(500);
            count += 1;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
