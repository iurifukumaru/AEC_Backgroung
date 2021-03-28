package stm

class TripRoute[L,R] (val join1: L => String) (val join2: R => String) extends Join[L, R, JoinClass] {
  override def join(a: List[L], b: List[R]): List[JoinClass] = {
    val l: Map[String, R] = b.map(b => join2(b) -> b).toMap
    a.filter(a => l.contains(join1(a))).map(a => JoinClass(a, Some(l(join1(a)))))
  }
}
