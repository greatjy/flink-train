package com.health.alibaba.flink_demo_ch8;

import java.util.Random;

/**
 * @Author: wjy
 * @Date: 2020/4/6 12:39
 */
public class MangshenTest {
    public static void main(String[] args) {
        Random random = new Random();
        int random1 = random.nextInt(100);
        int random2 = random.nextInt(100);
        System.out.println(random1 + " "+random2);
        if(random1 >= 95) {
            System.out.println("wangjiayang 学校盲审");
            if(random2 >= 95) {
                System.out.println("王佳扬既抽中了校盲审，又抽中院盲审");
            }
        }
        else if(random2 >= 95) {
            System.out.println("wangjiayang 学院盲审");
        }
        else{
            System.out.println("王佳扬没有中盲审");
        }


    }

}
