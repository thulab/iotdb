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
package org.apache.iotdb.tsfile.exception.write;

/**
 * This exception is threw while meeting error in writing procession.
 *
 * @author kangrong
 */
public class WriteProcessException extends Exception {
    private static final long serialVersionUID = -2664638061585302767L;
    protected String errMsg;

    public WriteProcessException(String msg) {
        super(msg);
        this.errMsg = msg;
    }

    @Override
    public String getMessage() {
        return errMsg;
    }
}
