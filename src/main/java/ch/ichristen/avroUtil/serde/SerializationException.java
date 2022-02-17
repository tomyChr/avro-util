/*
 * Copyright 2018 the original author or authors.
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

package ch.ichristen.avroUtil.serde;

/**
 * Signals that some data serialization exception of some sort has occurred.
 * This class is the general exception thrown during the execution of serialization/deserialization operations.
 *
 * @author Thomas Christen
 * @see Serializer
 * @see Deserializer
 */
public class SerializationException extends Exception {

  /**
   * @param message text description related to the possible cause of an issue
   * @param cause   info related to the cause saved for later retrieval
   */
  public SerializationException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * @param message text description related to the possible cause of an issue
   */
  public SerializationException(final String message) {
    super(message);
  }

  /* avoid the expensive and useless stack trace for serialization exceptions */
  @Override
  public Throwable fillInStackTrace() {
    return this;
  }

}
