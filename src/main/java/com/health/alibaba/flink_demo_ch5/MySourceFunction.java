package com.health.alibaba.flink_demo_ch5;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author: wjy
 * @Date: 2020/4/1 0:10
 */
public class MySourceFunction implements SourceFunction<String> {

    /**
     * 标记变量，是否正在运行，还是取消了
     */
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int count = 0;
         while(isRunning) {
             ctx.collect("just test addSource(SourceFunction object)"+count);
             Thread.sleep(500);
             count += 1;
         }
    }

    @Override
    public void cancel() {
         isRunning = false;
    }
}
