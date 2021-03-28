package stm

class EnrichedTrip[Left,Right] (val join1: (Left,Right) => Boolean) extends Join[Left,Right,JoinClass] {
  override def join(a: List[Left], b: List[Right]): List[JoinClass] = for {
    left <- a
    right <- b
    if join1(left, right)
  } yield JoinClass(left, Some(right))
}