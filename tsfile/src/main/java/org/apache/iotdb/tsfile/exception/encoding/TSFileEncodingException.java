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
package org.apache.iotdb.tsfile.exception.encoding;

import org.apache.iotdb.tsfile.exception.TSFileRuntimeException;

/**
 * This Exception is used while encoding failed. <br>
 * This Exception extends super class {@link TSFileRuntimeException}
 *
 * @author kangrong
 */
public class TSFileEncodingException extends TSFileRuntimeException {
    private static final long serialVersionUID = -7225811149696714845L;

    public TSFileEncodingException() {
    }

    public TSFileEncodingException(String message, Throwable cause) {
        super(message, cause);
    }

    public TSFileEncodingException(String message) {
        super(message);
    }

    public TSFileEncodingException(Throwable cause) {
        super(cause);
    }
}
