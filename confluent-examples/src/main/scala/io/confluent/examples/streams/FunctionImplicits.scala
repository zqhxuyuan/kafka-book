/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams

import org.apache.kafka.streams.kstream.Reducer

import scala.language.implicitConversions

/**
  * Implicit conversions that provide us with some syntactic sugar when writing stream transformations.
  */
object FunctionImplicits {

  implicit def BinaryFunctionToReducer[V](f: ((V, V) => V)): Reducer[V] = (l: V, r: V) => f(l, r)

}