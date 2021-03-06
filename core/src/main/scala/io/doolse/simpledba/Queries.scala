package io.doolse.simpledba

trait WriteQueries[S[_], F[_], W, T] {
  self =>

  protected def S: Streamable[S, F]

  def insertAll: S[T] => S[W]

  def updateAll: S[(T, T)] => S[W]

  def insert(t: T): S[W] = insertAll(S.emit(t))

  def update(old: T, next: T): S[W] = updateAll(S.emit((old, next)))

  def replaceAll[K](f: T => K): (S[T], S[T]) => S[W] =
    (exstream, newstream) => {
      S.flatMapS {
        S.foldLeft(exstream, Map.empty[K, T])((m, t) => m.updated(f(t), t))
      } { exmap =>
        S.flatMapS {
          S.foldLeft(newstream, (S.empty[W], exmap)) { (m, t) =>
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

  def insertUpdateOrDelete(existing: Option[T], newvalue: Option[T]): S[W] =
    (existing, newvalue) match {
      case (None, Some(v))      => insert(v)
      case (Some(ex), Some(nv)) => update(ex, nv)
      case (Some(old), None)    => delete(old)
      case _                    => S.empty
    }

  def deleteAll: S[T] => S[W]

  def delete(t: T) : S[W] = deleteAll(S.emit(t))
}
