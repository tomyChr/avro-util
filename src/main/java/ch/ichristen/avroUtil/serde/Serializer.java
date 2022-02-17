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

import java.util.Iterator;

/**
 * Generic serialization.
 * 
 * @author Björn Beskow
 * @author Thomas Christen
 */
public interface Serializer<T> {

  /**
   * Serialize object as byte array.
   * @param data the object to serialize
   * @return byte[]
   */
  byte[] serialize(T data) throws SerializationException;

  /**
   * Serialize objects as byte array.
   * @param data the iterator for the objects to serialize
   * @return byte[]
   */
  byte[] serialize(Iterator<T> data) throws SerializationException;

}