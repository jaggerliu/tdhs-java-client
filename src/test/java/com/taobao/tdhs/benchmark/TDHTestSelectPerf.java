/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package com.taobao.tdhs.benchmark;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.taobao.tdhs.client.TDHSClient;
import com.taobao.tdhs.client.TDHSClientImpl;

/**
 * 
 * @author shenzhw
 * 
 */
public class TDHTestSelectPerf {

	private  AtomicLong finshiedCount = new AtomicLong();
	private  AtomicLong failedCount = new AtomicLong();
	private  long start;

	private  ExecutorService executor = null;


	public  SimpleConPool getConPool(String url, String user, String password, int threadCount)
			throws SQLException, ClassNotFoundException {
		SimpleConPool conPool = new SimpleConPool(url, user, password, threadCount);
		return conPool;
	}

	private  void doTest(String url, String user, String password, int threadCount,
			long minId, long maxId, int executetimes, final boolean outmidle) {
		executor = Executors.newFixedThreadPool(threadCount);
		String host = url.split(":")[0];
		String port = url.split(":")[1];
		String db = url.split(":")[2];
		start = System.currentTimeMillis();
		for (int i = 0; i < threadCount; i++) {
			try {

				TDHSClient client = new TDHSClientImpl(new InetSocketAddress(host,
                        Integer.parseInt(port)), 1, 3000, false, 3000);
				TDHTravelRecordSelectJob job = new TDHTravelRecordSelectJob(client,db, minId, maxId,
						executetimes, finshiedCount, failedCount);
				executor.execute(job);
			} catch (Exception e) {
				System.out.println("failed create thread " + i + " err " + e.toString());
			}
		}
		System.out.println("success create thread count: " + threadCount);
		System.out.println("all thread started,waiting finsh...");
		 try {
			report();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		Class.forName("com.mysql.jdbc.Driver");
		if (args.length < 5) {
			System.out
					.println("input param,format: [jdbcurl] [user] [password]  [threadpoolsize]  [executetimes] [minId-maxId] [repeat]");
			return;
		}
		int threadCount = 0;// 线程数
		String url = args[0];
		String user = args[1];
		String password = args[2];
		threadCount = Integer.parseInt(args[3]);
		int executetimes = Integer.parseInt(args[4]);
		long minId = Integer.parseInt((args[5].split("-"))[0]);
		long maxId = Integer.parseInt((args[5].split("-"))[1]);
		System.out.println("concerent threads:" + threadCount);
		System.out.println("execute sql times:" + executetimes);
		System.out.println("maxId:" + maxId);
		int repeate = 1;
		if (args.length > 6) {
			repeate = Integer.parseInt(args[6]);
			System.out.println("repeat test times:" + repeate);
		}
//		for (int i = 0; i < repeate; i++) {
			try {
				new TDHTestSelectPerf().doTest(url, user, password, threadCount, minId, maxId, executetimes, repeate < 2);
			} catch (Exception e) {
				e.printStackTrace();
			}
//		}

	}

	public  void report() throws InterruptedException {
		executor.shutdown();

		SimpleDateFormat df = new SimpleDateFormat("dd HH:mm:ss");
		while (!executor.isTerminated()) {
				long sucess = finshiedCount.get() - failedCount.get();
				System.out.println(df.format(new Date())
						+ " finished :" + finshiedCount.get()
						+ " failed:" + failedCount.get() + " speed:" + sucess
						* 1000.0 / (System.currentTimeMillis() - start));
			Thread.sleep(1000);
		}

		long usedTime = (System.currentTimeMillis() - start) / 1000;
		System.out.println("finishend:" + finshiedCount.get() + " failed:"
				+ failedCount.get());
		long sucess = finshiedCount.get() - failedCount.get();
		System.out.println("used time total:" + usedTime + "seconds");
		System.out.println("qps:" + sucess / (usedTime+0.1));
		
	}
}