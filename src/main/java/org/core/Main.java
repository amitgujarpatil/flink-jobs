package org.core;

import java.util.concurrent.CompletableFuture;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("Hello threads");

        CompletableFuture<String> getIds = CompletableFuture.supplyAsync(()->{
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return  "amit is good at multi threading";
        });

        CompletableFuture<String> getStatus = CompletableFuture.supplyAsync(()->{
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return  "ACTIVE";
        });

        CompletableFuture<Void> idsAndStatusFuture =  CompletableFuture.allOf(getIds,getStatus);

        idsAndStatusFuture.thenRun(()->{
            String ids = getIds.join();
            String status = getStatus.join();

            System.out.println(ids);
            System.out.println(status);
        }).join();


    }




}
