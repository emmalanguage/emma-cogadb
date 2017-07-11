/*
 * Copyright © 2016 TU Berlin (emma@dima.tu-berlin.de)
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
package org.emmalanguage

import runtime.CoGaDB

trait CoGaDBAware {

  protected def setupCoGaDB(config: CoGaDB.Config): CoGaDB =
    CoGaDB(config)

  protected def destroyCoGaDB(cogadb: CoGaDB): Unit =
    cogadb.destroy()

  def withCoGaDB[T](f: CoGaDB => T): T = {
    val cogadb = setupCoGaDB(CoGaDB.Config())
    val result = f(cogadb)
    destroyCoGaDB(cogadb)
    result
  }

}
