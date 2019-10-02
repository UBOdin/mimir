package mimir.provenance

import java.sql.SQLException
import com.typesafe.scalalogging.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.util._
import mimir.optimizer._
import mimir.views.ViewAnnotation

class ProvenanceError(e:String) extends Exception(e) {}

object Provenance extends LazyLogging {

  val mergeRowIdFunction = ID("mimir_make_rowid")
  val rowidColnameBase = ID("MIMIR_ROWID")
  def rowidColname(x:Int) = ID(rowidColnameBase, "_"+x)
  
  def compile(oper: Operator): (Operator, Seq[ID]) = {
    val makeRowIDProjectArgs = 
      (rowids: Seq[ID], offset: Integer, padLen: Integer) => {
        rowids.map(Var(_)).
               padTo(padLen, RowIdPrimitive("-")).
               zipWithIndex.map( { case (v, i) => 
                  val newName = rowidColname(i+offset)
                  (newName, ProjectArg(newName, v))
               }).
               unzip
    }
    oper match {
      case Project(args, src) => {
            val (newSrc, rowIDCols) = compile(src)

            logger.trace(s"PROJECT: $rowIDCols in \n$oper")

            val newArgs = 
              args.map( arg => 
                ProjectArg(arg.name, expandVars(arg.expression, rowIDCols))
              )
            val (newRowids, rowIDProjections) = makeRowIDProjectArgs(rowIDCols, 0, 0)
            (
              Project(newArgs ++ rowIDProjections, newSrc),
              newRowids
            )
          // }
        // }
      }
      
      case Select(cond, src) => {
        val (newSrc, rowids) = compile(src)
        ( 
          Select(expandVars(cond, rowids), newSrc), 
          rowids
        )
      }

      case Join(lhs, rhs) => {
        val (newLhs, lhsRowids) = compile(lhs)
        val (newRhs, rhsRowids) = compile(rhs)

        logger.trace(s"JOIN: $lhsRowids || $rhsRowids in\n$oper")

        val (newLhsRowids, lhsIdProjections) = 
          makeRowIDProjectArgs(lhsRowids, 0, 0)
        val (newRhsRowids, rhsIdProjections) = 
          makeRowIDProjectArgs(rhsRowids, lhsRowids.size, 0)
        val lhsProjectArgs =
          lhs.columnNames.map(x => ProjectArg(x, Var(x))) ++ lhsIdProjections
        val rhsProjectArgs = 
          rhs.columnNames.map(x => ProjectArg(x, Var(x))) ++ rhsIdProjections
        (
          Join(
            Project(lhsProjectArgs, newLhs),
            Project(rhsProjectArgs, newRhs)
          ),
          newLhsRowids ++ newRhsRowids
        )
      }

      case Union(lhs, rhs) => {
        val (newLhs, lhsRowids) = compile(lhs)
        val (newRhs, rhsRowids) = compile(rhs)
        
        logger.trace(s"UNION: $lhsRowids || $rhsRowids in\n$oper")

        val (newRowids, lhsIdProjections) = 
          makeRowIDProjectArgs(lhsRowids, 0, rhsRowids.size)
        val (_,         rhsIdProjections) = 
          makeRowIDProjectArgs(rhsRowids, 0, lhsRowids.size)
        val lhsProjectArgs =
          lhs.columnNames.map(x => ProjectArg(x, Var(x))) ++ 
            lhsIdProjections ++ 
            Seq(ProjectArg(ID(rowidColnameBase,"_BRANCH"), RowIdPrimitive("0")))
        val rhsProjectArgs = 
          rhs.columnNames.map(x => ProjectArg(x, Var(x))) ++ 
            rhsIdProjections ++ 
            Seq(ProjectArg(ID(rowidColnameBase,"_BRANCH"), RowIdPrimitive("1")))
        (
          Union(
            Project(lhsProjectArgs, newLhs),
            Project(rhsProjectArgs, newRhs)
          ),
          newRowids ++ Seq(ID(rowidColnameBase,"_BRANCH"))
        )
      }

      case View(name, query, meta) => val (newQuery, rowIds) = compile(query)
        ( View(name, newQuery, meta + ViewAnnotation.PROVENANCE), rowIds)
      
      case AdaptiveView(model, name, query, meta) => 
        val (newQuery, rowIds) = compile(query)
        ( AdaptiveView(model, name, newQuery, meta + ViewAnnotation.PROVENANCE), rowIds)

      case Table(name, source, schema, meta) =>
        (
          Table(name, source, schema, meta ++ List((rowidColnameBase, RowIdVar(), TRowId()))),
          List(rowidColnameBase)
        )

      case empty@HardTable(schema,Seq()) =>
        (
          empty,
          List()
        )

      case ht@HardTable(sch,data) =>
        (
          HardTable(
            sch:+(ID(rowidColnameBase,"_HT"),TRowId()), 
            data.zipWithIndex.map(row => row._1:+ RowIdPrimitive(s"hardcoded${row._2}"))),
          List(ID(rowidColnameBase,"_HT"))
        )

      case Aggregate(groupBy, args, child) =>
        //val newargs = (new AggregateArg(ROWID_KEY, List(Var(ROWID_KEY)), ROWID_KEY)) :: args
        ( 
          Aggregate(groupBy, args, compile(child)._1),
          groupBy.map(_.name)
        )

      case Sort(sortCols, child) => {
        val (rewritten, cols) = compile(child)
        (Sort(sortCols, rewritten), cols)
      }

      case Limit(offset, count, child) => {
        val (rewritten, cols) = compile(child)
        (Limit(offset, count, rewritten), cols)
      }

      case LeftOuterJoin(lhs, rhs, cond) => {
        val (newLhs, lhsRowids) = compile(lhs)
        val (newRhs, rhsRowids) = compile(rhs)

        logger.trace(s"OUTER JOIN: $lhsRowids || $rhsRowids in\n$oper")

        val (newLhsRowids, lhsIdProjections) = 
          makeRowIDProjectArgs(lhsRowids, 0, 0)
        val (newRhsRowids, rhsIdProjections) = 
          makeRowIDProjectArgs(rhsRowids, lhsRowids.size, 0)
        val lhsProjectArgs =
          lhs.columnNames.map(x => ProjectArg(x, Var(x))) ++ lhsIdProjections
        val rhsProjectArgs = 
          rhs.columnNames.map(x => ProjectArg(x, Var(x))) ++ rhsIdProjections

        val newOuterJoin =           
          LeftOuterJoin(
            Project(lhsProjectArgs, newLhs),
            Project(rhsProjectArgs, newRhs),
            cond
          )

        val outerJoinWithSafeRHS =
          newOuterJoin.alterColumnsByID(
            newRhsRowids.map { id =>
              id -> Var(id).isNull.thenElse { RowIdPrimitive("-") } { Var(id) }
            }:_*
          )

        ( outerJoinWithSafeRHS, newLhsRowids ++ newRhsRowids )
      }

    }
  }
  
  
  
  def rowIdVal(rowids: Seq[Expression]): Expression =
    Function(mergeRowIdFunction, rowids)    

  def rowIdVar(rowids: Seq[ID]): Expression = 
    rowIdVal(rowids.map(Var(_)))

  def expandVars(expr: Expression, rowidFields: Seq[ID]): Expression = {
    expr match {
      case RowIdVar() => rowIdVar(rowidFields)
      case _ => expr.rebuild(expr.children.map(expandVars(_,rowidFields)))
    }
  }

  def plugInToken(expr: Expression, rowid: RowIdPrimitive): Expression = {
    expr match {
      case RowIdVar() => rowid
      case _ => expr.rebuild(expr.children.map(plugInToken(_,rowid)))
    }
  }

  def joinRowIds(rowids: Seq[PrimitiveValue]): RowIdPrimitive =
  {
    logger.debug(s"JOIN ROWIDS: $rowids")
    RowIdPrimitive(rowids.map(_.asString).mkString("|"))
  }

  def splitRowIds(token: RowIdPrimitive): Seq[RowIdPrimitive] =
    token.asString.split("\\|").map( RowIdPrimitive(_) ).toList

  def rowIdMap(token: RowIdPrimitive, rowIdFields:Seq[ID]):Map[ID,RowIdPrimitive] = 
    rowIdMap(splitRowIds(token), rowIdFields).toMap

  def rowIdMap(token: Seq[RowIdPrimitive], rowIdFields:Seq[ID]):Map[ID,RowIdPrimitive] =
    rowIdFields.zip(token).toMap

  def filterForToken(operator:Operator, token: RowIdPrimitive, rowIdFields: Seq[ID], db: Database): Operator =
    filterForToken(operator, rowIdMap(token, rowIdFields), db)

  def filterForToken(operator:Operator, token: Seq[RowIdPrimitive], rowIdFields: Seq[ID], db: Database): Operator =
    filterForToken(operator, rowIdMap(token, rowIdFields), db)

  def filterForToken(operator:Operator, rowIdsByColumn: Map[ID,PrimitiveValue], db: Database): Operator =
  {
    // Distributivity of unions makes this particular rewrite a little
    // tricky.  Specifically, UNION might assign either 'left' or 'right' to one
    // of the parent rowIds, depending on which branch we go through.  Unfortunately,
    // we might need to go arbitrarilly deep into the operator tree before we 
    // discover the projection where the attribute is hardcoded.  We deal with
    // this in the same way that parser constructors work: The return value of 
    // the recursive process is an Option.
    // 
    // If we hit a branch of the union where the specific rowId is not properly set,
    // then we take the other branch.
    // Projects will either return Some() if they're in that branch, or None, if
    // there's a hardcoded rowid column that doesn't match the target field.
    // At the very end, we strip off the final Option.

    doFilterForToken(operator, rowIdsByColumn, db) match {
      case Some(s) => s
      case None => throw new ProvenanceError("No branch matching all union terms")
    }

  }

  def doFilterForToken(operator: Operator, rowIdsByColumn:Map[ID,PrimitiveValue], db: Database): Option[Operator] =
  {
    logger.trace(s"doFilterForToken($rowIdsByColumn) in \n$operator")
    // println(rowIds.toString+" -> "+operator)
    operator match {
      case p @ Project(args, src) =>

        // The projection might remap or hardcode specific rowId column
        // names, so read through and figure out which columns are which
        // Variable columns are passed through to the recursive step
        // Constant columns are tested -- 
        val (rowIdVars, rowIdConsts) =
          rowIdsByColumn.keys.map( col => 
            p.get(col) match {
              case Some(Var(v)) => (Some((col, v)), None)
              case Some(RowIdPrimitive(v)) => (None, Some((col, v)))
              case unknownExpr => 
                throw new ProvenanceError("Operator not properly compiled for provenance: Projection Column "+col+" has expression "+unknownExpr)
            }).unzip

        val newRowIdMap =
          rowIdVars.flatten.map( x => (x._2, rowIdsByColumn(x._1) ) ).toMap

        if(rowIdConsts.flatten.forall({ case (col, v) => 
          // println("COMPARE: "+rowIds(col).asString+" to "+v)
          rowIdsByColumn(col).asString.equals(v)
        })) {
          doFilterForToken(src, newRowIdMap, db).
            map(Project(args, _))
        } else {
          None
        }

      case Select(cond, src) => 
        // technically not necessary... since we're already filtering down to
        // a single tuple.  But keep it here for now.
        doFilterForToken(src, rowIdsByColumn, db).map( Select(cond, _) )

      case Join(lhs, rhs) => 
        val lhsSchema = lhs.columnNames.toSet
        val (lhsRowIds, rhsRowIds) = 
          rowIdsByColumn.toList.partition( x => lhsSchema.contains(x._1) )
        // println("LHS: "+lhsRowIds)
        // println("RHS: "+rhsRowIds)
        ( doFilterForToken(lhs, lhsRowIds.toMap, db), 
          doFilterForToken(rhs, rhsRowIds.toMap, db) 
        ) match {
          case (Some(newLhs), Some(newRhs)) => Some(Join(newLhs, newRhs))
          case _ => None
        }
        

      case Union(lhs, rhs) => 
        doFilterForToken(lhs, rowIdsByColumn, db).
          orElse(doFilterForToken(rhs, rowIdsByColumn, db))

      // We don't handle materializing the entire history of a given value
      // for now... drop the view and focus on the query itself.
      case View(_, query, _) => 
        doFilterForToken(query, rowIdsByColumn, db)
      case AdaptiveView(_, _, query, _) => 
        doFilterForToken(query, rowIdsByColumn, db)

      case Table(_, _, _, meta) =>
        meta.find( _._2.equals(RowIdVar()) ) match {
          case Some( (colName, _, _) ) =>
            var rowIdForTable = rowIdsByColumn.get(colName) match {
              case Some(s) => s
              case None =>
                throw new ProvenanceError("Token missing for Table: "+colName+" in "+rowIdsByColumn)
            }
            Some(Select( 
              Comparison(Cmp.Eq, Var(colName), rowIdsByColumn(colName)), 
              operator 
            ))
          case None => 
            throw new ProvenanceError("Operator not compiled for provenance: "+operator)
        }

      case HardTable(sch,Seq()) => None 
  
      case HardTable(sch,data) => {
        val cols = sch.unzip._1
        val tupleMap = data.map(row => cols.zip(row).toMap)
        val rowIdKeys = cols.toSet & rowIdsByColumn.keySet
        tupleMap.foldLeft(Seq[Seq[PrimitiveValue]]())((init, row) => {
            if(rowIdKeys.forall { key =>  row(key).equals(rowIdsByColumn(key))}){
              init :+ row.toSeq.unzip._2
            } else init
          }) match {
            case Seq() => None
            case newData => Some(HardTable(sch, newData))
        }
      }
      
      case Aggregate(gbCols, aggCols, src) =>
        val sch = db.typechecker.schemaOf(src).toMap

        val castTokenValues = 
          gbCols.map { col => (col.name, Cast(sch(col.name), rowIdsByColumn(col.name))) }.toMap

        val lookupFilter = 
          ExpressionUtils.makeAnd(
            gbCols.map( col => 
              Comparison(Cmp.Eq,
                col,
                castTokenValues(col.name)
              )
            )
          )
        return Some(
          Project(
            gbCols.map { col => ProjectArg(col.name, castTokenValues(col.name)) } ++
              aggCols.map { col => ProjectArg(col.alias, Var(col.alias)) },
            Aggregate(List(), aggCols, 
              Select(lookupFilter, src)
            )
          )
        )

      case Sort(_, src) => 
        // Sorts are irrelevant here, drop it
        return doFilterForToken(src, rowIdsByColumn, db)

      case Limit(_, _, src) => 
        // A limit would make this query invalid, drop it
        return doFilterForToken(src, rowIdsByColumn, db)

      case _:LeftOuterJoin => 
        throw new RAException("Provenance can't handle left outer joins")

    }
  }
}
