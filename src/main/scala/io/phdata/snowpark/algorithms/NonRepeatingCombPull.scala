package io.phdata.snowpark.algorithms

class NonRepeatingCombPull[T](domain: List[T], genSize: Int)  extends Iterable[List[T]] {

  case class StackFrame(partDomain: List[T], curSize: Int, partResult: List[T], callOrder: Int)

  class MyStack[S] {
    private var rep: List[S] = Nil
    def push(t: S): Unit = rep = t :: rep
    def pop(): Unit = rep = rep.tail
    def top(): S = rep.head
    def empty(): Boolean = rep.isEmpty
  }

  var hasNextVar = true
  var stack = new MyStack[StackFrame]()
  stack.push(StackFrame(domain.reverse, genSize, Nil, 2))
  var callOrder = 0

  def progress(): Any = {
    if (stack.top().partDomain.size == stack.top().curSize) callOrder = 3
    else callOrder = 1
    if (stack.top().curSize == 0 && callOrder == 2) callOrder = 3
    do {
      if (stack.top().partDomain.isEmpty) callOrder = 3
      if (stack.top().curSize == 0 && callOrder == 2) callOrder = 3
      callOrder match {
        case 1 =>
          stack.push(StackFrame(stack.top().partDomain.tail, stack.top().curSize, stack.top().partResult, 1))
        case 2 =>
          stack.push(StackFrame(stack.top().partDomain.tail, stack.top().curSize - 1, stack.top().partDomain.head :: stack.top().partResult, 2))
          callOrder = 1
        case 3 =>
          callOrder = stack.top().callOrder + 1
          stack.pop()
          hasNextVar = !stack.empty()
      }
    } while (hasNextVar && stack.top().partDomain.size != stack.top().curSize)
  }

  def iterator: Iterator[List[T]] = new Iterator[List[T]] {
    def hasNext(): Boolean = hasNextVar
    def next(): List[T] = {
      val result = stack.top().partDomain.reverse ::: stack.top().partResult
      progress()
      result
    }
    progress()
  }
}
