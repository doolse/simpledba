package io.doolse.simpledba

trait WriteOp

trait Flushable[S[_]]
{
  def flush: S[WriteOp] => S[Unit]
}

trait WriteQueries[S[_], F[_], T] {
  self =>

  protected def S: Streamable[S, F]

  def insertAll: S[T] => S[WriteOp]

  def updateAll: S[(T, T)] => S[WriteOp]

  def insert(t: T): S[WriteOp] = insertAll(S.emit(t))

  def update(old: T, next: T): S[WriteOp] = updateAll(S.emit((old, next)))

  def replaceAll[K](f: T => K): (S[T], S[T]) => S[WriteOp] =
    (exstream, newstream) => {
      val M = S.SM

      M.flatMap {
        S.scan(exstream, Map.empty[K, T])((m, t) => m.updated(f(t), t))
      } { exmap =>
        M.flatMap {
          S.scan(newstream, (S.empty[WriteOp], exmap)) { (m, t) =>
            val k  = f(t)
            val ex = exmap.get(k)
            ex match {
              case None      => m.copy(_1 = S.append(m._1, insert(t)))
              case Some(exT) => (S.append(m._1, update(exT, t)), m._2 - k)
            }
          }
        } { case (w, toDelete) => S.append(w, deleteAll(S.emits(toDelete.values.toSeq))) }
      }
    }

  def insertUpdateOrDelete(existing: Option[T], newvalue: Option[T]): S[WriteOp] =
    (existing, newvalue) match {
      case (None, Some(v))      => insert(v)
      case (Some(ex), Some(nv)) => update(ex, nv)
      case (Some(old), None)    => delete(old)
      case _                    => S.empty
    }

  def deleteAll: S[T] => S[WriteOp]

  def delete(t: T) = deleteAll(S.emit(t))
}
