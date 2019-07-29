package io.doolse.simpledba

trait WriteQueries[S[-_, _], F[-_, _], -R, W, T] {
  self =>

  protected def S: Streamable[S, F]

  def insertAll[R1 <: R]: S[R1, T] => S[R1, W]

  def updateAll[R1 <: R]: S[R1, (T, T)] => S[R1, W]

  def insert(t: T): S[R, W] = insertAll(S.emit(t))

  def update(old: T, next: T): S[R, W] = updateAll(S.emit((old, next)))

  def replaceAll[R1 <: R, K](f: T => K): (S[R1, T], S[R1, T]) => S[R1, W] =
    (exstream, newstream) => {
      S.flatMapS {
        S.foldLeft(exstream, Map.empty[K, T])((m, t) => m.updated(f(t), t))
      } { exmap =>
        S.flatMapS {
          S.foldLeft(newstream, (S.empty[R1, W], exmap)) { (m, t) =>
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

  def insertUpdateOrDelete(existing: Option[T], newvalue: Option[T]): S[R, W] =
    (existing, newvalue) match {
      case (None, Some(v))      => insert(v)
      case (Some(ex), Some(nv)) => update(ex, nv)
      case (Some(old), None)    => delete(old)
      case _                    => S.empty
    }

  def deleteAll[R1 <: R]: S[R1, T] => S[R1, W]

  def delete(t: T) : S[R, W] = deleteAll(S.emit(t))
}
