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
package compiler.lang.cogadb

import net.liftweb.json._
import org.scalatest.FreeSpec
import org.scalatest.Matchers

import java.io.FileNotFoundException

/** A spec for the [[runtime.CoGaDB]] runtime. */
class JSerializerSpec extends FreeSpec with Matchers {

  "JSON Serializing" - {

    "predicates" in {
      val exp = parse(readResourceFile("/plans/predicates.json"))
      val act = fold(JSerializer)(ast.And(Seq(
        ast.ColCol(
          lhs = ast.AttrRef("lineitem", "order_id", "l_order_id"),
          rhs = ast.AttrRef("order", "id", "o_order_id"),
          cmp = ast.Equal
        ),
        ast.ColConst(
          attr = ast.AttrRef("order", "amount", "amount"),
          const = ast.IntConst(500),
          cmp = ast.Unequal
        ))))

      act shouldEqual exp
    }

    "table scan" in {
      val exp = parse(readResourceFile("/plans/tablescan.json"))
      val act = fold(JSerializer)(ast.Root(
        ast.TableScan("CUSTOMER")))

      act shouldEqual exp
    }

    "aggregation specification" in {
      val exp = parse(readResourceFile("/plans/aggspec.json"))
      val act = fold(JSerializer)(ast.AggFuncSimple("SUM", ast.AttrRef("ORDER", "AMOUNT", "AMOUNT"), "REVENUE"))

      act shouldEqual exp
    }

    "group by" in {
      val exp = parse(readResourceFile("/plans/groupby.json"))
      val act = fold(JSerializer)(ast.GroupBy(
        Seq(
          ast.AttrRef("LINEITEM", "L_ORDERKEY", "L_ORDERKEY"),
          ast.AttrRef("ORDERS", "O_ORDERDATE", "O_ORDERDATE"),
          ast.AttrRef("ORDERS", "O_SHIPPRIORITY", "O_SHIPPRIORITY")),
        Seq(
          ast.AggFuncSimple("SUM", ast.AttrRef("ORDER", "AMOUNT", "AMOUNT"), "REVENUE")),
        ast.TableScan("CUSTOMER")))

      act shouldEqual exp
    }

    "sort by" in {
      val exp = parse(readResourceFile("/plans/sortby.json"))
      val act = fold(JSerializer)(ast.Root(
        ast.Sort(
          Seq(
            ast.SortCol("<COMPUTED>", "REVENUE", "DOUBLE", "REVENUE", 1, "DESCENDING"),
            ast.SortCol("ORDERS", "O_ORDERDATE", "DATE", "O_ORDERDATE", 1, "ASCENDING")),
          ast.TableScan("CUSTOMER"))))

      act shouldEqual exp
    }

    "join" in {
      val exp = parse(readResourceFile("/plans/join.json"))
      val act = fold(JSerializer)(ast.Root(
        ast.Join("INNER_JOIN",
          Seq(ast.ColCol(
            lhs = ast.AttrRef("SUPPLIER", "S_SUPPKEY", "S_SUPPKEY"),
            rhs = ast.AttrRef("PARTSUPP", "PS_SUPPKEY", "PS_SUPPKEY"),
            cmp = ast.Equal)),
          ast.TableScan("SUPPLIER"),
          ast.TableScan("PARTSUPP"))))

      act shouldEqual exp
    }

    "export" in {
      val exp = parse(readResourceFile("/plans/export.json"))
      val act = fold(JSerializer)(ast.Root(
        ast.ExportToCsv("filename.csv", ",", ast.TableScan("CUSTOMER"))))

      act shouldEqual exp
    }

    "group by with sum and reduce udf" in {
      val exp = parse(readResourceFile("/plans/groupby_with_agg_funcs.json"))

      val groupBy2 = ast.GroupBy(
        Seq(
          ast.AttrRef("LINEORDER", "LO_SHIPMODE", "SHIPMODE", 1)
        ),
        Seq(
          ast.AggFuncSimple("SUM",
            ast.AttrRef("LINEORDER", "LO_REVENUE", "SUM_REVENUE", 1), "SUM_REVENUE"),
          ast.AggFuncReduce(
            ast.AlgebraicReduceUdf(
              Seq(
                ast.ReduceUdfPayAttrRef("double", "min_value", ast.DoubleConst(400000)),
                ast.ReduceUdfPayAttrRef("OID", "ID", ast.IntConst(0))
              ),
              Seq(
                ast.ReduceUdfOutAttr("OID", "CID", "TMP_CID")
              ),
              Seq(
                ast.ReduceUdfCode("if(#<hash_entry>.min_value#>#LINEORDER.LO_REVENUE#){"),
                ast.ReduceUdfCode("   #<hash_entry>.min_value#=#LINEORDER.LO_REVENUE#;"),
                ast.ReduceUdfCode("   #<hash_entry>.ID#=#LINEORDER.LO_LINENUMBER#;"),
                ast.ReduceUdfCode("}")
              ),
              Seq(
                ast.ReduceUdfCode("#<out>.CID# = #<hash_entry>.min_value#*#<hash_entry>.ID#;")
              )
            )
          )
        ),
        ast.TableScan("LINEORDER", 1)
      )
      val act = fold(JSerializer)(groupBy2)

      act shouldEqual exp
    }

  }

  private def readResourceFile(p: String): String =
    Option(getClass.getResourceAsStream(p))
      .map(scala.io.Source.fromInputStream)
      .map(_.getLines.toList.mkString("\n"))
      .getOrElse(throw new FileNotFoundException(p))
}
