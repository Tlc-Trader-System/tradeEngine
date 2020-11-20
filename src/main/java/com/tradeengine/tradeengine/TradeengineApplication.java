package com.tradeengine.tradeengine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@SpringBootApplication
public class TradeengineApplication {

	public static void main(String[] args) {

		ValidatedOrder validatedOrder = new ValidatedOrder("1011","IBM","1.5","1000","SELL");
		String validatedOrderString = Utility.convertToString(validatedOrder);
		new Jedis().lpush("makeorder",validatedOrderString);
		System.out.println(validatedOrderString);
		SpringApplication.run(TradeengineApplication.class, args);

		//	MAKE ORDERBOOK REQUEST
		new Thread(new Runnable() {
			Jedis jedis = new Jedis();
			@Override
			public void run() {
	    		while (true){
					String data = jedis.rpop("makeorder");
					if(data == null) continue;
					ValidatedOrder validatedOrder = Utility.convertToObject(data,ValidatedOrder.class);
					jedis.set(validatedOrder.id,data);
					jedis.lpush("monitorqueue",validatedOrder.id);
					OrderBookRequest orderBookRequest = new OrderBookRequest(
							validatedOrder.id,
							validatedOrder.product,
							validatedOrder.side
					);
					String requestString = Utility.convertToString(orderBookRequest);
					jedis.lpush("exchange1-orderrequest",requestString);
					jedis.lpush("exchange2-orderrequest",requestString);
				}
			}
		}).start();





		//	MONITOR QUEUE
		new Thread(new Runnable() {
			Jedis jedis = new Jedis();
			@Override
			public void run() {
				while (true){
					String data = jedis.rpop("monitorqueue");
					if(data == null) continue;
					ValidatedOrder validatedOrder=Utility.convertToObject(data,ValidatedOrder.class );
					if(jedis.llen(data+"orderbook") == 2){

						PendingOrder[] pendingOrderList1 = Utility.convertToObject(jedis.rpop(data+"orderbook"),PendingOrder[].class);
						PendingOrder[] pendingOrderList2 = Utility.convertToObject(jedis.rpop(data+"orderbook"),PendingOrder[].class);
						/*
						 *Logic
						 * Get Exchange orders into one list
						 * Sort list by price
						 * if (side==Buy){
						 * sort  Asc
						 * else
						 * sort desc
						 * Start popping from top till order quantity is fulfilled
						 * }
						 *  */

						//Exchange Orders into one list
						List<PendingOrder> pendingOrderList= new ArrayList(Arrays.asList(pendingOrderList1,pendingOrderList2));

						//sorting
						if (validatedOrder.side == "Buy") {
							pendingOrderList.sort(Comparator.comparing(PendingOrder::getPrice));
						} else {
							pendingOrderList.sort(Comparator.comparing(PendingOrder::getPrice).reversed());
						}

						//Popping and fulfilling orders
						int orderquantity =0;
						PendingOrder curExchangeOrder;
						Object orderInfo;
						int targetQty =Integer.valueOf( validatedOrder.quantity);
						while (orderquantity< targetQty){
							curExchangeOrder = pendingOrderList.remove(0);
							if(Integer.valueOf(curExchangeOrder.quantity)>targetQty) {
//									orderInfo = curExchangeOrder
									orderquantity+=Integer.valueOf(curExchangeOrder.quantity);
							}
							else{
								orderquantity+=Integer.valueOf(curExchangeOrder.quantity);
							}

						}


//						ValidatedOrder fulfiiledOrder = Utility.convertToObject( jedis.get(data), ValidatedOrder.class);

						jedis.lpush("makeorder"+"exchange1");

					}else{
						jedis.lpush("monitorqueue",data);
					}
				}
			}
		}).start();


	}



}



