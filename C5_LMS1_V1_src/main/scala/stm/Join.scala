package stm

trait Join[Left,Right,Output] {
  def join(a: List[Left], b: List[Right]): List[Output]
}

case class JoinClass(left: Any, right: Option[Any])
