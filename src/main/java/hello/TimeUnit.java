/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hello;

public enum TimeUnit {
	ns(1),
	ms(1000),
	s(1000L * 1000),
	m(1000L * 1000 * 60),
	h(1000L * 1000 * 60 * 60)
	;

	private long nanos;

	TimeUnit(long nanos) {
		this.nanos = nanos;
	}

	public double convert(long howMany, TimeUnit original) {
		return (double) howMany * original.nanos / this.nanos;

	}
}
