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

import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class PropertyUtils {

  private PropertyUtils() {}

  public static boolean propertyAsBoolean(
      Map<String, String> properties, String property, boolean defaultValue) {
    String value = properties.get(property);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultValue;
  }

  public static Optional<Boolean> propertyAsOptionalBoolean(
      Map<String, String> properties, String property) {
    String value = properties.get(property);
    if (value != null) {
      return Optional.of(Boolean.parseBoolean(value));
    }
    return Optional.empty();
  }

  public static double propertyAsDouble(
      Map<String, String> properties, String property, double defaultValue) {
    String value = properties.get(property);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return defaultValue;
  }

  public static int propertyAsInt(
      Map<String, String> properties, String property, int defaultValue) {
    String value = properties.get(property);
    if (value != null) {
      return Integer.parseInt(value);
    }
    return defaultValue;
  }

  public static Optional<Integer> propertyAsOptionalInt(
      Map<String, String> properties, String property) {
    String value = properties.get(property);
    if (value != null) {
      return Optional.of(Integer.parseInt(value));
    }
    return Optional.empty();
  }

  public static long propertyAsLong(
      Map<String, String> properties, String property, long defaultValue) {
    String value = properties.get(property);
    if (value != null) {
      return Long.parseLong(value);
    }
    return defaultValue;
  }

  public static Optional<Long> propertyAsOptionalLong(
      Map<String, String> properties, String property) {
    String value = properties.get(property);
    if (value != null) {
      return Optional.of(Long.parseLong(value));
    }
    return Optional.empty();
  }

  public static String propertyAsString(
      Map<String, String> properties, String property, String defaultValue) {
    String value = properties.get(property);
    if (value != null) {
      return value;
    }
    return defaultValue;
  }

  public static String propertyAsString(Map<String, String> properties, String property) {
    String value = properties.get(property);
    ValidationUtils.checkNotNull(value, "Property %s must be set", property);
    return value;
  }

  public static Optional<String> propertyAsOptionalString(
      Map<String, String> properties, String property) {
    return Optional.ofNullable(properties.get(property));
  }

  /**
   * Returns subset of provided map with keys matching the provided prefix. Matching is
   * case-sensitive and the matching prefix is removed from the keys in returned map.
   *
   * @param properties input map
   * @param prefix prefix to choose keys from input map
   * @return subset of input map with keys starting with provided prefix and prefix trimmed out
   */
  public static Map<String, String> propertiesWithPrefix(
      Map<String, String> properties, String prefix) {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptyMap();
    }

    ValidationUtils.checkArgument(prefix != null, "Invalid prefix: null");

    return properties.entrySet().stream()
        .filter(e -> e.getKey().startsWith(prefix))
        .collect(Collectors.toMap(e -> e.getKey().replaceFirst(prefix, ""), Map.Entry::getValue));
  }

  /**
   * Filter the properties map by the provided key predicate.
   *
   * @param properties input map
   * @param keyPredicate predicate to choose keys from input map
   * @return subset of input map with keys satisfying the predicate
   */
  public static Map<String, String> filterProperties(
      Map<String, String> properties, Predicate<String> keyPredicate) {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptyMap();
    }

    ValidationUtils.checkArgument(keyPredicate != null, "Invalid key pattern: null");

    return properties.entrySet().stream()
        .filter(e -> keyPredicate.test(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
