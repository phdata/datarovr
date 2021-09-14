package io.phdata.snowpark.helpers

/**
 * Takes a glob pattern and returns a standard regex.
 * Taken and ported from here:
 * https://stackoverflow.com/a/17369948/2639647

 * @param glob glob pattern to convert
 */
case class Glob(glob: String) {
  private val buffer = new StringBuilder(glob.length)

  private var inGroup = 0;
  private var inClass = 0;
  private var firstIndexInClass = -1;

  private val escape = Seq('.', '(', ')', '+', '|', '^', '$', '@', '%')

  private var i = 0
  while(i < glob.length) {
    glob(i) match {
      case '\\' =>
        if (i+1 >= glob.length) {
          buffer.append('\\')
        } else {
          glob(i+1) match {
            case ',' => ;
            case x => {
              if (x == 'Q' || x == 'E') buffer.append('\\')
              buffer.append('\\')
            }
          }
          buffer.append(glob(i+1))
          i += 1
        }
      case '*' => if (inClass == 0) buffer.append(".*") else buffer.append('*')
      case '?' => if (inClass == 0) buffer.append(".") else buffer.append('?')
      case '[' =>
        inClass += 1
        firstIndexInClass = i+1
        buffer.append('[')
      case ']' =>
        inClass -= 1
        buffer.append(']')
      case x if escape contains x =>
        if ((inClass == 0) || (firstIndexInClass == i && glob(i) == '^')) {
          buffer.append('\\')
        }
        buffer.append(x)
      case '!' => if (firstIndexInClass == i) buffer.append('^') else buffer.append('!')
      case '{' =>
        inGroup += 1
        buffer.append('(')
      case '}' =>
        inGroup -= 1
        buffer.append(')')
      case ',' => if (inGroup > 0) buffer.append('|') else buffer.append(',')
      case x => buffer.append(x)
    }
    i += 1
  }

  val r = ("^"+buffer+"$").r

  override def toString: String = {
    buffer.result
  }
}
