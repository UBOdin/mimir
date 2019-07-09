package mimir.algebra;

object QueryNamer 
{
	def apply(op: Operator) = nameQuery(op)

	def nameQuery(op: Operator): ID = 
	{
		op match { 
			case Table(name,alias, _, _, _) => name
			case View(name, _, _) => name
			case AdaptiveView(model, name, _, _) => model+ID("_")+name
			case Project(cols, src) => 
				cols.length match {
					case 1 => cols(0).name+ID("_FROM_")+nameQuery(src)
					case 2 => 
						cols(0).name+ID("_AND_")+
						cols(1).name+ID("_FROM_")+
						nameQuery(src)
					case _ => nameQuery(src)
				}
			case Select(cond, src) =>
				nameQuery(src)+ID("_WHERE_")+nameBoolExpression(cond)
			case Join(lhs, rhs) =>
				if(hasJoinOrUnion(lhs) || hasJoinOrUnion(rhs)) {
					foldIDs(getRelationNames(lhs)++getRelationNames(rhs), "_")
				} else {
					nameQuery(lhs)+ID("_")+nameQuery(rhs)
				}
			case LeftOuterJoin(lhs, _, _) =>
				nameQuery(lhs)
			case Union(lhs, rhs) =>
				if(hasJoinOrUnion(lhs) || hasJoinOrUnion(rhs)) {
					foldIDs(getRelationNames(lhs)++getRelationNames(rhs), "_PLUS_")
				} else {
					nameQuery(lhs)+ID("_PLUS_")+nameQuery(rhs)
				}
			case Aggregate(_, _, src) =>
				ID(nameQuery(src), "_SUMMARIZED")
			case Limit(_, _, src) =>
				nameQuery(src)
			case Sort(cols, src) =>
				nameQuery(src)+ID("_BY_")+foldIDs(
					cols.flatMap(col => ExpressionUtils.getColumns(col.expression)),
					"_"
				)
			case HardTable(_,Seq()) => 
				ID("EMPTY_QUERY")
			case HardTable(_,_) => 
				ID("HARDCODED")
		}
	}


	def foldIDs(ids: Seq[ID], sep: String): ID =
		if(ids.isEmpty) { ID("") }
		else { ids.tail.foldLeft(ids.head) {
			(old, curr) => old+ID(sep)+curr
		}}

	def nameArithExpression(e: Expression): ID =
	{
		ID("AN_EXPRESSION")
	}

	def nameBoolExpression(e: Expression): ID =
	{
		ID("A_CONDITION_HOLDS")
	}

	def hasJoinOrUnion(q: Operator): Boolean =
	{
		q match { 
			case Join(_,_) => true
			case Union(_,_) => true
			case _ => q.children.map( hasJoinOrUnion(_) ).foldLeft(false)( _ || _ )
		}
	}
	def getRelationNames(q: Operator): Seq[ID] =
	{
		q match {
			case Table(tn, ta, _, _, _) => Seq(tn)
			case _ => q.children.map( getRelationNames(_) ).flatten
		}
	}


}
