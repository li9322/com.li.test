package com.li.spark.java;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPoolDemo {
    public static void main(String[] args){
       ExecutorService pool=Executors.newCachedThreadPool();
//       ExecutorService pool=Executors.newSingleThreadExecutor();
//       ExecutorService pool=Executors.newFixedThreadPool(5);
       for (int i=0;i<=10;i++){
           int finalI = i;
           pool.execute(new Runnable() {
               @Override
               public void run() {
                   System.out.println(Thread.currentThread().getName()+"===="+ finalI);
                   try {
                       Thread.sleep(5000);
                   } catch (InterruptedException e) {
                       e.printStackTrace();
                   }
                   System.out.println(Thread.currentThread().getName()+"======"+ finalI+"====is over");
               }
           });
       }
       System.out.println("all thread is over");
       pool.shutdown();
    }
}
