package mimir.algebra

class RegisterFunctions {

  def RegisterFunctions(conn:java.sql.Connection):Unit = {
    org.sqlite.Function.create(conn,"MIMIRCAST", new MimirCast())
    org.sqlite.Function.create(conn,"OTHERTEST", new OtherTest())
    org.sqlite.Function.create(conn,"AGGTEST", new aggTest())
  }
}

class MimirCast extends org.sqlite.Function {
    @Override
    def xFunc(): Unit = { // 1 is int, double is 2, 3 is string, 5 is null
      if (args != 2) { throw new java.sql.SQLDataException("NOT THE RIGHT NUMBER OF ARGS FOR MIMIRCAST, EXPECTED 2 IN FORM OF MIMIRCAST(COLUMN,TYPE)") }
      try {
        if(value_int(1) == 1){ // int
          if(value_type(0) == 1){
            result(value_int(0))
          }
          else if(value_type(0) == 2){
            result(value_double(0))
          }
          else{
            result() // else return null
          }
        }
        else if(value_int(1) == 2){ // double
            if(value_type(0) == 1){
              result(value_int(0))
            }
            else if(value_type(0) == 2){
              result(value_double(0))
            }
            else{
              result() // else return null
            }
        }
        else if (value_int(1) == 3){ // String
          result(value_text(0))
        }
        else if(value_int(1) == 4){ // maybe boolean? not really sure
          result("CHECK MIMIRCAST, TELL ME WHAT VALUE THIS IS... TELL ME!")
        }
        else if(value_int(1) == 5){ // null
          result()
        }
        else if(value_int(1) == 0){ // 0 might be a type but I assume it was cast to 0 from a string
          result("I assume that you put something other than a number in, this functions works like, MIMIRCAST(column,type), the types are int values, 1 is int, 2 is double, 3 is string, and 5 is null, so MIMIRCAST(COL,1) is casting column 1 to int")
        }
        else{
          throw new java.sql.SQLDataException("Well here we are, I'm not sure really what went wrong but it happened in MIMIRCAST, maybe it was a type, good luck")
        }

      } catch {
        case _: java.sql.SQLDataException => throw new java.sql.SQLDataException();
      }
    }
}

class OtherTest extends org.sqlite.Function {
  @Override
  def xFunc(): Unit = {
    try {
      result(8000);
    } catch {
      case _: java.sql.SQLDataException => throw new java.sql.SQLDataException();
    }
  }
}

class aggTest extends org.sqlite.Function.Aggregate {

  var total = 0
  @Override
  def xStep(): Unit ={
    total = total + value_int(0)
  }

  def xFinal(): Unit ={
    result(total)
  }
}