package io.doolse.simpledba

import shapeless.HList
import shapeless.ops.record.SelectAll

/**
  * Created by jolz on 8/06/16.
  */
case class RelationDef[T, CR <: HList, KL <: HList, CVL <: HList]
(baseName: String, mapper: ColumnMapper[T, CR, CVL])(implicit ev: SelectAll[CR, KL])
