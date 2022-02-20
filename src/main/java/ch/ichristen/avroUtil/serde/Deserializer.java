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

import org.apache.avro.specific.SpecificRecord;

import java.util.Collection;

/**
 * Generic deserialization.
 *
 * @author Thomas Christen
 * @author Björn Beskow
 */
public interface Deserializer {

  /**
   * Deserialize object from a byte array.
   * @param clazz the expected class for the deserialized object
   * @param data the byte array
   * @return Object instance of type clazz
   */
  Object deserialize(Class clazz, byte[] data) throws SerializationException;

  /**
   * Deserialize objects from a byte array into a collection
   * @param clazz the expected class for the deserialized object
   * @param data the byte array
   * @return Collection object instance
   */
  Collection<SpecificRecord> deserializeCollection(Class<? extends SpecificRecord> clazz, byte[] data) throws SerializationException;

}