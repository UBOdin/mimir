package mimir.optimizer;

import java.sql._;

import mimir.algebra._;
import mimir.ctables._;

object InlineProjections {

	def optimize(o: Operator): Operator = 
	{

//		println("OPER: " + o)
		o match {

			case table @ mimir.algebra.Table(tableName:String,columns:List[(String,Type.T)],rowIDList:List[(String,Expression,Type.T)]) => {

				if(columns.size == 0){
					return o
				}

				val proj:List[ProjectArg] = columns.map((arg) => {

					var ret:ProjectArg = null
					if(arg._2 == Type.TString){
						ret = ProjectArg(arg._1,new mimir.algebra.Function("MIMIRCAST",List(Var(arg._1),IntPrimitive(3))))
					}
					else if(arg._2 == Type.TInt){
						ret = ProjectArg(arg._1,new mimir.algebra.Function("MIMIRCAST",List(Var(arg._1),IntPrimitive(1))))
					}
					else if(arg._2 == Type.TFloat){
						ret = ProjectArg(arg._1,new mimir.algebra.Function("MIMIRCAST",List(Var(arg._1),IntPrimitive(2))))
					}
					else{
						ret = ProjectArg(arg._1,new mimir.algebra.Function("MIMIRCAST",List(Var(arg._1),IntPrimitive(3))))
					}
					ret
				}
				)

				var projectPayLoad:List[ProjectArg] = null

				if(rowIDList.size != 0){
					 projectPayLoad = proj:+ProjectArg(rowIDList(0)._1,Var(rowIDList(0)._1))
				}
				else{
					projectPayLoad = proj
				}

				val oper = Project(projectPayLoad,table)
//				println("NEW OPER: "+oper)
				oper
				}

/*			case pr @ Project(cols,src:mimir.algebra.Table) => { //println("COLS: "+ cols); println("SRC: " + src)
				cols.map((arg:ProjectArg) => println("EXPRESSION: "+arg.expression.getClass)); o
				println("Bindings: "+pr.bindings)
				println(src.schema.map((x)=> println("Schema: "+x)))
				val schema = src.schema
				optimize(Project(cols.map((arg) => {

					var place:Int = -1
					var counter = 0
					if(arg.expression.toString.contains("MIMIRCAST")){
						return pr
					}
					schema.map((i) => {
							if(arg.expression.toString.equals(i._1)){
								place = counter
							}
						else{
							println(arg.expression.toString + " != "+ i._1)
						}
						counter+=1

					})

//					if(place == -1){
//						println("OH NO PLACE HASN'T BEEN FOUND IN INLINEPROJ")
//					}

					var ret:ProjectArg = null
					if(schema(place)._2 == Type.TString){
						ret = ProjectArg(arg.name,new mimir.algebra.Function("MIMIRCAST",List(arg.expression,IntPrimitive(3))))
					}
					else if(schema(place)._2 == Type.TInt){
						ret = ProjectArg(arg.name,new mimir.algebra.Function("MIMIRCAST",List(arg.expression,IntPrimitive(1))))
					}
					else if(schema(place)._2 == Type.TFloat){
						ret = ProjectArg(arg.name,new mimir.algebra.Function("MIMIRCAST",List(arg.expression,IntPrimitive(2))))
					}
					else if(schema(place)._2 == Type.TRowId){
						ret = ProjectArg(arg.name,arg.expression)
					}
					else{
						ret = ProjectArg(arg.name,new mimir.algebra.Function("MIMIRCAST",List(arg.expression,IntPrimitive(3))))
					}
					ret
				}
				),src))
			}
*/
			case Project(cols, src) if (cols.forall( _ match {
				case ProjectArg(colName, Var(varName)) => colName.equals(varName)
				case _ => false
			}) && (src.schema.map(_._1).toSet &~ cols.map(_.name).toSet).isEmpty)

			 => optimize(src)

			case Project(cols, p @ Project(_, src)) =>
				val bindings = p.bindings;
				optimize(Project(
					cols.map( (arg:ProjectArg) =>
						ProjectArg(arg.name, Eval.inline(arg.expression, bindings))
					),
					src
				))

			case Project(cols, src) => 
				// println("Inline : " + o)
				Project(
					cols.map( (arg:ProjectArg) =>
						ProjectArg(arg.name, Eval.inline(arg.expression))
					),
					optimize(src)
				)

			case _ => o.rebuild(o.children.map(optimize(_)))

		}
	}

}