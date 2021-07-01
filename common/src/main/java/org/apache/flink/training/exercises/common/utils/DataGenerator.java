/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.common.utils;

import java.time.Instant;
import java.util.Random;

/**
 * Data generator for the fields in TaxiRide and TaxiFare objects.
 *
 * <p>Results are deterministically determined by the rideId. This guarantees (among other things)
 * that the startTime for a TaxiRide START event matches the startTime for the TaxiRide END and
 * TaxiFare events for that same rideId.
 */
/**
 * TaxiRide 和 TaxiFare 对象中字段的数据生成器。
 *
 * <p>结果由rideId确定。 这保证（除其他外）
 * TaxiRide START 事件的 startTime 与 TaxiRide END 的 startTime 匹配，并且
 * 保证相同rideId的TaxiFare事件。
 */
public class DataGenerator {

	private static final int SECONDS_BETWEEN_RIDES = 20; //每两单之间间隔20秒
	private static final int NUMBER_OF_DRIVERS = 200;
	private static final Instant beginTime = Instant.parse("2020-01-01T12:00:00.00Z");

	private transient long rideId; //关键字段

	/**
	 * Creates a DataGenerator for the specified rideId.
	 * 为指定的rideId 创建一个DataGenerator。
	 */
	public DataGenerator(long rideId) {
		this.rideId = rideId;
	}

	/**
	 * Deterministically generates and returns the startTime for this ride.
	 * 确定性地生成并返回此行程的开始时间。
	 */
	public Instant startTime() {
		//public Instant plusSeconds(long secondsToAdd)
		return beginTime.plusSeconds(SECONDS_BETWEEN_RIDES * rideId); // Instant类的plusSeconds()方法，接受一个参数secondsToAdd，这是要添加的秒数。
	}

	/**
	 * Deterministically generates and returns the endTime for this ride.
	 * 确定性地生成并返回此行程的结束时间。
	 */
	public Instant endTime() {
		return startTime().plusSeconds(60 * rideDurationMinutes());
	}

	/**
	 * Deterministically generates and returns the driverId for this ride.
	 * The HourlyTips exercise is more interesting if aren't too many drivers.
	 * 确定性地生成并返回此行程的 driverId。
	 */
	public long driverId() {
		Random rnd = new Random(rideId);
		return 2013000000 + rnd.nextInt(NUMBER_OF_DRIVERS);
	}

	/**
	 * Deterministically generates and returns the taxiId for this ride.
	 * 确定性地生成并返回此行程的出租车 ID。
	 */
	public long taxiId() {
		return driverId();
	}

	/**
	 * Deterministically generates and returns the startLat for this ride.
	 * 确定性地生成并返回此行程的 startLat
	 *
	 * <p>The locations are used in the RideCleansing exercise.
	 * We want some rides to be outside of NYC.
	 *
	 * 这些位置用于 RideCleansing 练习。
	 * 我们希望一些游乐设施在纽约市以外。
	 */
	public float startLat() {
		return aFloat((float) (GeoUtils.LAT_SOUTH - 0.1), (float) (GeoUtils.LAT_NORTH + 0.1F));
	}

	/**
	 * Deterministically generates and returns the startLon for this ride.
	 */
	public float startLon() {
		return aFloat((float) (GeoUtils.LON_WEST - 0.1), (float) (GeoUtils.LON_EAST + 0.1F));
	}

	/**
	 * Deterministically generates and returns the endLat for this ride.
	 */
	public float endLat() {
		return bFloat((float) (GeoUtils.LAT_SOUTH - 0.1), (float) (GeoUtils.LAT_NORTH + 0.1F));
	}

	/**
	 * Deterministically generates and returns the endLon for this ride.
	 */
	public float endLon() {
		return bFloat((float) (GeoUtils.LON_WEST - 0.1), (float) (GeoUtils.LON_EAST + 0.1F));
	}

	/**
	 * Deterministically generates and returns the passengerCnt for this ride.
	 */
	public short passengerCnt() {
		return (short) aLong(1L, 4L);
	}

	/**
	 * Deterministically generates and returns the paymentType for this ride.
	 * 确定性地生成并返回此行程的 paymentType。
	 */
	public String paymentType() {
		return (rideId % 2 == 0) ? "CARD" : "CASH";
	}

	/**
	 * Deterministically generates and returns the tip for this ride.
	 *
	 * <p>The HourlyTips exercise is more interesting if there's some significant variation in tipping.
	 * 确定性地生成并返回这次骑行的小费。
	 * * <p>如果小费有一些显着的变化，HourlyTips 练习会更有趣。
	 */
	public float tip() {
		return aLong(0L, 60L, 10F, 15F);
	}

	/**
	 * Deterministically generates and returns the tolls for this ride.
	 * 确定性地生成并返回此行程的通行费。
	 */
	public float tolls() {
		return (rideId % 10 == 0) ? aLong(0L, 5L) : 0L;
	}

	/**
	 * Deterministically generates and returns the totalFare for this ride.
	 * 确定性地生成并返回此行程的总票价。
	 */
	public float totalFare() {
		return (float) (3.0 + (1.0 * rideDurationMinutes()) + tip() + tolls());
	}

	/**
	 * The LongRides exercise needs to have some rides with a duration > 2 hours, but not too many.
	 * LongRides 运动需要有一些持续时间 > 2 小时的骑行，但不要太多。
	 */
	private long rideDurationMinutes() {
		return aLong(0L, 600, 20, 40);
	}

	// -------------------------------------

	private long aLong(long min, long max) {
		float mean = (min + max) / 2.0F;
		float stddev = (max - min) / 8F;

		return aLong(min, max, mean, stddev);
	}

	// the rideId is used as the seed to guarantee deterministic results
	// 将rideId用作种子以保证确定性结果
	private long aLong(long min, long max, float mean, float stddev) {
		Random rnd = new Random(rideId);
		long value;
		do {
			value = (long) Math.round((stddev * rnd.nextGaussian()) + mean); // The Math.round() function returns the value of a number rounded to the nearest integer.
		} while ((value < min) || (value > max));
		return value;
	}

	// -------------------------------------

	private float aFloat(float min, float max) {
		float mean = (min + max) / 2.0F;
		float stddev = (max - min) / 8F;

		return aFloat(rideId, min, max, mean, stddev);
	}

	private float bFloat(float min, float max) {
		float mean = (min + max) / 2.0F;
		float stddev = (max - min) / 8F;

		return aFloat(rideId + 42, min, max, mean, stddev);
	}

	// the rideId is used as the seed to guarantee deterministic results
	private float aFloat(long seed, float min, float max, float mean, float stddev) {
		Random rnd = new Random(seed);
		float value;
		do {
			value = (float) (stddev * rnd.nextGaussian()) + mean;
		} while ((value < min) || (value > max));
		return value;
	}

}
