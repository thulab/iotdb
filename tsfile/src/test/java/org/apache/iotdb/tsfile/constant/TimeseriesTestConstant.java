/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.constant;

import java.util.Random;

/**
 * This class is used for Junit test to get some unified constant.
 * 
 * @author kangrong
 *
 */
public class TimeseriesTestConstant {
    public static final float float_min_delta = 0.00001f;
    public static final double double_min_delta = 0.00001d;
    public static final Random random = new Random(System.currentTimeMillis());
}
