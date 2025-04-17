/*
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
package com.lancedb.lance.spark.utils;

public class ValidationUtils {

  private ValidationUtils() {}

  public static void checkArgument(boolean expression, String message, Object... args) {
    if (!expression) {
      throw new IllegalArgumentException(String.format(message, args));
    }
  }

  public static void checkNotNull(Object object, String message, Object... args) {
    checkArgument(object != null, message, args);
  }

  public static void checkNotNullOrEmptyString(String str, String message, Object... args) {
    checkArgument(str != null && !str.isEmpty(), message, args);
  }

  public static void checkState(boolean expression, String message, Object... args) {
    if (!expression) {
      throw new IllegalStateException(String.format(message, args));
    }
  }
}
