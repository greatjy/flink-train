package com.health.alibaba.flink_demo_ch5;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @Author: wjy
 * @Date: 2020/4/1 0:42
 */
public class MyRichParallelSourceFunction extends RichParallelSourceFunction<String> {
    private boolean isRunning = true;
    private int count = 0;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        while(isRunning) {
            ctx.collect("hello"+count);
            Thread.sleep(1000);
            count += 1;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
