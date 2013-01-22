/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datasalt.pangool.io;

/**
 * Used by {@link Tuple#deepCopy(ITuple, java.util.Map)} to allow
 * deep copy of objects that Pangool don't know how to copy.
 */
public interface FieldClonator {

  /**
   * Must return a new copy of the given value.
   *
   * @param value The objecto to copy.
   * @return A new instance that is a copy of the given value.
   */
  public Object giveMeACopy(Object value);
}
