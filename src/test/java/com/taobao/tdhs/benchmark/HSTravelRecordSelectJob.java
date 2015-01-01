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

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;


public class HSTravelRecordSelectJob implements Runnable {
	private final HandlerSocket hs;
	private final long minId;
	private final long maxId;
	private final int executeTimes;
	private final AtomicLong finshiedCount;
	private final AtomicLong failedCount;
	Random random = new Random();
	private final String db;

	// private volatile long usedTime;
	// private volatile long success;

	public HSTravelRecordSelectJob(HandlerSocket hs, String db, long minId, long maxId, int executeTimes,
			AtomicLong finshiedCount, AtomicLong failedCount) {
		super();
		this.db = db;
		this.hs = hs;
		this.minId = minId;
		this.maxId = maxId;
		this.executeTimes = executeTimes;
		this.finshiedCount = finshiedCount;
		this.failedCount = failedCount;
	}


	private void select() {
		

		String idValue = ((Math.abs(random.nextLong()) % (maxId - minId)) + minId)+"";
		try{
			hs.command().openIndex("id", db, "travelrecord", "PRIMARY", "id,user_id,traveldate,fee,days");

			hs.command().find("id", new String[]{idValue});
			List<HandlerSocketResult> results = hs.execute();

			for(HandlerSocketResult result : results){
				System.out.println("\t" + result.toString());
			}
	
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
		}
		
//		ResultSet rs = null;
//		Connection conn = null;
//		PreparedStatement ps = null;
		try {

//			conn = conPool.getConnection();
//			 String sql = "select id,user_id,traveldate,fee,days from  travelrecord  where id="
//			 + ((Math.abs(random.nextLong()) % (maxId - minId)) + minId);
//			 ps = conn.prepareStatement(sql);
//			 rs = ps.executeQuery(sql);
			
//			  TDHSResponse r = client.get(db, "travelrecord", "id", new String[] { "name" },
//	    				new String[][] { { "aaa" } }, FindFlag.TDHS_EQ, 0, 100,
//	    				new Filter[] { f });
//			  TDHSResponse response = client.query().use(db).from("travelrecord")
//	                   .select("id", "user_id", "traveldate", "fee", "days")
//	                   .where().fields("id").equal(idValue).get();

//			String sql = "select * from  travelrecord  where id=?";
//			ps = conn.prepareStatement(sql);
//			ps.setLong(1, (Math.abs(random.nextLong()) % (maxId - minId)) + minId);
//			rs = ps.executeQuery();
			
			finshiedCount.incrementAndGet();
			// success++;
		} catch (Exception e) {
			failedCount.incrementAndGet();
			e.printStackTrace();
		} finally {
//			try {
//				if (ps != null)
//					ps.close();
//				if (rs != null)
//					rs.close();
//				conPool.returnCon(conn);
//			} catch (SQLException e) {
//			}
		}
	}

	public void run() {
		// long start = System.currentTimeMillis();
		for (int i = 0; i < executeTimes; i++) {
			this.select();
			// usedTime = System.currentTimeMillis() - start;
		}
		

		try {
			hs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// public long getUsedTime() {
	// return this.usedTime;
	// }

	// public int getTPS() {
	// if (usedTime > 0) {
	// return (int) (this.success * 1000 / this.usedTime);
	// } else {
	// return 0;
	// }
	// }

	public static void main(String[] args) {
		Random r = new Random();
		for (int i = 0; i < 10; i++) {
			int f = r.nextInt(90000 - 80000) + 80000;
			System.out.println(f);
		}
	}
}