package org.apache.rocketmq.test.djl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CompletableFutureTest {
    public static void main(String[] args) {

        final ExecutorService executorService = Executors.newFixedThreadPool(4);

        CompletableFuture<String> cf1 = CompletableFuture.supplyAsync(() -> {
//            try {
//                TimeUnit.SECONDS.sleep(1);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            return "ok";
        }, executorService);

        // 测试结果得知: cf1如果已经完成则注册的 thenAccept 则由当前线程执行,否则由回调线程执行
        cf1.thenAccept(x -> {
            // Thread.currentThread().getName() = main
            System.out.println("Thread.currentThread().getName() = " + Thread.currentThread().getName());
            System.out.println("x = " + x);
        }).join();


    }
}
