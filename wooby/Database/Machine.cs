using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using wooby.Database.Persistence;
using wooby.Database.Defaults;
using wooby.Error;
using wooby.Parsing;

namespace wooby.Database
{
    public class Machine
    {
        public Context Context { get; private set; }

        private static readonly List<Operator> BooleanOperators = new List<Operator>
        {
            Operator.Equal, Operator.NotEqual, Operator.LessEqual, Operator.MoreEqual, Operator.MoreThan,
            Operator.LessThan, Operator.And, Operator.Or
        };

        private static bool OperatorIsBoolean(Operator op)
        {
            return BooleanOperators.Contains(op);
        }

        private static bool OperatorIsLowerBoolean(Operator op)
        {
            return op == Operator.And || op == Operator.Or;
        }

        public Context Initialize()
        {
            Initialize(new Context());

            return Context;
        }

        public void Initialize(Context context)
        {
            Context = context;
            InitializeFunctions();
            InitializeTables();
        }

        private void InitializeFunctions()
        {
            var types = new List<Type>
            {
                typeof(CurrentDate_Function),
                typeof(DatabaseName_Function),
                typeof(RowNum_Function),
                typeof(RowId_Function),
                typeof(Trunc_Function),
                typeof(Count_Function),
                typeof(Min_Function),
                typeof(Max_Function),
                typeof(Sum_Function),
            };

            for (int id = 0; id < types.Count; id++)
            {
                var func = Activator.CreateInstance(types[id], new object[] {id}) as Function;
                Context.AddFunction(func);
            }
        }

        private void InitializeTables()
        {
            Context.ResetNotReal();

            var dualMeta = new TableMeta()
            {
                Name = "dual",
                Columns = new List<ColumnMeta>(),
                IsReal = false,
                IsTemporary = false,
                DataProvider = new Dual_DataProvider()
            };
            Context.AddTable(dualMeta);

            var loveliveMeta = new TableMeta() {Name = "lovelive", IsReal = false, DataProvider = new LoveLive_DataProvider()}
                .AddColumn("id", ColumnType.Number)
                .AddColumn("parent_id", ColumnType.Number)
                .AddColumn("nome", ColumnType.String)
                .AddColumn("ano", ColumnType.Number)
                .AddColumn("integrantes", ColumnType.Number);
            Context.AddTable(loveliveMeta);
        }

        private void RegisterTable(TableMeta Meta, ITableDataProvider Provider)
        {
            Meta.DataProvider = Provider;
            Provider.Initialize(Context, Meta);
            Context.AddTable(Meta);
        }

        private static void CheckOutputRows(ExecutionContext context)
        {
            if (context.QueryOutput.Rows.Count == 1 && context.QueryOutput.Rows[0].Values.All(v => v is NullValue))
            {
                context.QueryOutput.Rows.RemoveAt(0);
            }
        }

        public class ColumnValueComparer : IComparer<BaseValue>, IEqualityComparer<BaseValue>
        {
            public int Compare(BaseValue x, BaseValue y)
            {
                if (x == null)
                {
                    return -1;
                } else if (y == null)
                {
                    return 1;
                } 
                return x.Compare(y);
            }

            public bool Equals(BaseValue x, BaseValue y)
            {
                return x != null && y != null && x.Compare(y) == 0;
            }

            public int GetHashCode([DisallowNull] BaseValue obj)
            {
                return base.GetHashCode();
            }
        }

        private static void PrepareQueryOutput(ExecutionContext exec, Expression expr)
        {
            string id;
            if (!string.IsNullOrEmpty(expr.Identifier))
            {
                id = expr.Identifier;
            } else if (expr.IsOnlyReference() && exec.Sources.Count == 1)
            {
                var re = expr.Nodes[0].ReferenceValue;
                id = re.Column;
            }
            else
            {
                id = expr.FullText;
            }

            exec.QueryOutput.Definition.Add(new OutputColumnMeta() {OutputName = id});
        }

        private static List<RowOrderingIntermediate> BuildFromRows(ExecutionContext exec, SelectStatement query, List<long> ids, int colIndex)
        {
            var result = new List<RowOrderingIntermediate>();
            var ascending = query.OutputOrder[colIndex].Kind == OrderingKind.Ascending;

            IEnumerable<TempRow> input;
            if (ids != null && ids.Any())
            {
                input = exec.TempRows.Where(r => ids.Contains(r.RowId));
            }
            else
            {
                input = exec.TempRows.AsEnumerable();
            }

            var reference = query.OutputOrder[colIndex].OrderExpression;
            if (!reference.IsOnlyReference())
            {
                throw new NotImplementedException();
            }
            var name = reference.Nodes[0].ReferenceValue.Join();
            var groups = input.GroupBy(row => row.EvaluatedReferences[name], new ColumnValueComparer()).OrderBy(r => r.Key, new ColumnValueComparer());
            var cursor = ascending ? groups : groups.Reverse();
            foreach (var group in cursor)
            {
                result.Add(new RowOrderingIntermediate()
                {
                    MatchingRows = group.Select(row => row.RowId).ToList()
                });
            }

            return result;
        }

        private static void OrderSub(ExecutionContext context, SelectStatement query, List<RowOrderingIntermediate> scope, int colIndex)
        {
            if (colIndex >= query.OutputOrder.Count)
                return;

            foreach (var items in scope)
            {
                if (items.MatchingRows.Count > 1)
                {
                    items.SubOrdering = BuildFromRows(context, query, items.MatchingRows, colIndex);
                    items.MatchingRows = null;
                }
            }
        }

        private static void OrderOutputRows(ExecutionContext exec, SelectStatement query)
        {
            if (query.OutputOrder.Count > 0)
            {
                var group = BuildFromRows(exec, query, null, 0);
                OrderSub(exec, query, group, 1);

                var newResult = new List<OutputRow>(exec.QueryOutput.Rows.Count);
                foreach (var item in group)
                {
                    item.Collect(exec, newResult);
                }

                exec.QueryOutput.Rows = newResult;
            }
        }

        private List<List<TempRow>> GroupRowsRecursive(ExecutionContext exec, List<TempRow> current, int level,
            SelectStatement query)
        {
            var groups = new Dictionary<string, List<TempRow>>();
            var flags = new EvaluationFlags() {Origin = ExpressionOrigin.Grouping, Phase = QueryEvaluationPhase.Final};

            foreach (var row in current)
            {
                var correspondingResult = EvaluateExpression(exec, query.Grouping[level], row, flags);
                var distinct = correspondingResult.PrettyPrint();
                if (groups.TryGetValue(distinct, out var sub))
                {
                    sub.Add(row);
                }
                else
                {
                    groups.Add(distinct, new List<TempRow> {row});
                }
            }

            if (level >= query.Grouping.Count - 1)
            {
                return groups.Values.ToList();
            }
            else
            {
                return groups.Values.SelectMany(rows => GroupRowsRecursive(exec, rows, level + 1, query)).ToList();
            }
        }

        private List<TempRow> FlattenTempRows(ExecutionContext exec, List<List<TempRow>> groups, SelectStatement query)
        {
            var result = new List<TempRow>();

            var flags = new EvaluationFlags {Origin = ExpressionOrigin.Ordering, Phase = QueryEvaluationPhase.Final};
            // For each sub group, we get one TempRow
            foreach (var subGroup in groups)
            {
                // Since it's grouped by GROUP BY and all columns are supposed to be aggregated,
                // just evaluate the ordering expressions that may not have already been pre-cached
                // because of aggregate functions using this sub group

                exec.TempRows = subGroup;
                foreach (var order in query.OutputOrder.Where(o => o.OrderExpression.HasAggregateFunction))
                {
                    EvaluateCurrentRowReferences(exec, order.OrderExpression, subGroup[0], flags);
                }

                result.Add(subGroup[0]);
            }

            return result;
        }

        private void DistinctRows(ExecutionContext exec, SelectStatement query)
        {
            // We could group the output again, but there's a concern regarding aggregate functions being executed
            // again, and altering it so that isn't a problem is probably too much for now
            // So the strategy is to hash each row and filter them

            if (query.Distinct)
            {
                exec.QueryOutput.Rows = exec.QueryOutput.Rows
                    .GroupBy(row => string.Join(',', row.Values.Select(col => col.PrettyPrint())))
                    .Select(g => g.First()).ToList();
            }
        }

        private void GroupRows(ExecutionContext exec, SelectStatement query)
        {
            exec.ResetRowNumber();
            var flags = new EvaluationFlags
                {Origin = ExpressionOrigin.OutputColumn, Phase = QueryEvaluationPhase.Final};

            if (query.Grouping.Count > 0)
            {
                var originalTempRows = exec.TempRows;
                var groups = GroupRowsRecursive(exec, originalTempRows, 0, query);
                foreach (var group in groups)
                {
                    var headId = group[0].RowId;
                    exec.MainSource.DataProvider.Seek(headId);
                    exec.TempRows = group;

                    var row = new OutputRow {RowId = headId};
                    // For each sub group, now generate one output row
                    foreach (var expr in query.OutputColumns)
                    {
                        exec.Stack.Clear();
                        row.Values.Add(EvaluateExpression(exec, expr, group[0], flags));
                        while (exec.Stack.Count > 0)
                        {
                            row.Values.Add(exec.PopStack());
                        }
                    }

                    exec.QueryOutput.Rows.Add(row);
                    exec.IncrementRowNumber();
                }

                exec.TempRows = FlattenTempRows(exec, groups, query);
            }
            else
            {
                foreach (var temp in exec.TempRows)
                {
                    var headId = temp.RowId;
                    exec.MainSource.DataProvider.Seek(headId);
                    var r = new OutputRow {RowId = temp.RowId};
                    // For each sub group, now generate one output row
                    foreach (var expr in query.OutputColumns)
                    {
                        exec.Stack.Clear();
                        r.Values.Add(EvaluateExpression(exec, expr, temp, flags));
                        while (exec.Stack.Count > 0)
                        {
                            r.Values.Add(exec.PopStack());
                        }
                    }

                    exec.QueryOutput.Rows.Add(r);
                    exec.IncrementRowNumber();

                    if (query.OutputColumns.Any(e => e.HasAggregateFunction))
                    {
                        // Since there isn't any grouping defined and an aggregate function was used,
                        // there will only be one resulting row in the output
                        break;
                    }

                    exec.MainSource.DataProvider.SeekNext();
                }
            }
        }

        private void ExecuteCreate(ExecutionContext exec, CreateStatement statement)
        {
            // For now, just add a table with the given name to the context and create a generic data provider

            var meta = new TableMeta
            {
                Name = statement.Name,
                IsReal = true,
                IsTemporary = false,
            };

            foreach (var col in statement.Columns)
            {
                meta.AddColumn(col.Name, col.Type);
            }

            RegisterTable(meta, PersistenceBackendHelper.GetTableDataProvider(exec.Context));
        }

        private void ExecuteInsert(ExecutionContext exec, InsertStatement insert)
        {
            if (insert.MainSource.Kind != TableSource.SourceKind.Reference)
            {
                throw new WoobyDatabaseException("Cannot run manipulation on temporary query table");
            }
            
            var newColumns = new Dictionary<int, BaseValue>();
            var table = exec.Context.FindTable(insert.MainSource.Reference);
            if (table == null)
            {
                throw new Exception("Could not find table");
            }

            if (insert.Columns.Count != 0 && insert.Columns.Count != insert.Values.Count ||
                (insert.Columns.Count == 0 && table.Columns.Count != insert.Values.Count))
            {
                throw new Exception("Error: Different length for Columns list as Values list");
            }

            var numCols = insert.Columns.Count == 0 ? table.Columns.Count : insert.Columns.Count;
            var flags = new EvaluationFlags {Origin = ExpressionOrigin.OutputColumn, Phase = QueryEvaluationPhase.Final};

            for (int i = 0; i < numCols; ++i)
            {
                int idx;
                if (insert.Columns.Count == 0)
                {
                    idx = i;
                }
                else
                {
                    idx = table.Columns.FindIndex(c => c.Name == insert.Columns[i]);
                }

                if (idx < 0)
                {
                    throw new Exception($"Could not find column {insert.Columns[i]}");
                }

                var value = EvaluateExpression(exec, insert.Values[i], null, flags);
                newColumns.Add(idx, value);
            }

            SetupSources(exec, insert);

            exec.MainSource.DataProvider.Insert(newColumns);
            exec.RowsAffected = 1;
        }

        private void ExecuteUpdate(ExecutionContext exec, UpdateStatement update)
        {
            SetupSources(exec, update);

            var flags = new EvaluationFlags {Origin = ExpressionOrigin.Filter, Phase = QueryEvaluationPhase.Final};

            exec.QueryOutput.Rows.Add(new());
            while (exec.MainSource.DataProvider.SeekNext())
            {
                if (update.FilterConditions != null)
                {
                    var filter = EvaluateExpression(exec, update.FilterConditions, null, flags);
                    if (filter is BooleanValue boolean)
                    {
                        if (!boolean.Value)
                        {
                            continue;
                        }
                    } else
                    {
                        throw new Exception("Expected boolean result for WHERE clause expression");
                    }
                }
                
                var dict = new Dictionary<int, BaseValue>();

                foreach (var col in update.Columns)
                {
                    var column = exec.Context.FindColumn(col.Item1);

                    if (column == null)
                    {
                        throw new Exception("Could not find referenced column");
                    }

                    var value = EvaluateExpression(exec, col.Item2, null, flags);
                    dict.Add(column.Id, value);
                }
                exec.MainSource.DataProvider.Update(dict);
                exec.RowsAffected += 1;
            }
        }

        private void ExecuteDelete(ExecutionContext exec, DeleteStatement delete)
        {
            SetupSources(exec, delete);
            int affected = 0;

            while (exec.MainSource.DataProvider.SeekNext())
            {
                if (delete.FilterConditions != null)
                {
                    var filter = EvaluateExpression(exec, delete.FilterConditions, null,
                        new EvaluationFlags {Origin = ExpressionOrigin.Filter, Phase = QueryEvaluationPhase.Final});
                    if (filter is BooleanValue boolean)
                    {
                        if (!boolean.Value)
                        {
                            continue;
                        }
                    } else
                    {
                        throw new Exception("Expected boolean result for WHERE clause expression");
                    }
                }
                affected += 1;
                exec.MainSource.DataProvider.Delete();
            }

            exec.RowsAffected = affected;
        }

        private ExecutionDataSource FindSourceByReference(ExecutionContext exec, ColumnReference reference)
        {
            return exec.Sources.Find(src => src.NameMatches(reference.Table));
        }

        private BaseValue ReadColumnReference(ExecutionContext exec, ColumnReference reference)
        {
            var sourceContext = GetContextForLevel(exec, reference.ParentLevel);
            if (sourceContext.RowNumber > sourceContext.TempRows.Count - 1 || !sourceContext
                    .TempRows[sourceContext.RowNumber].EvaluatedReferences
                    .TryGetValue(reference.Join(), out BaseValue value))
            {
                var source = FindSourceByReference(exec, reference);
                if (source.Matched)
                {
                    var col = source.Meta.FindColumn(reference);
                    value = source.DataProvider.Read(col.Id);
                }
                else
                {
                    value = new NullValue();
                }
            }

            return value;
        }

        private BaseValue EvaluateFunctionCall(ExecutionContext exec, FunctionCall call, TempRow? temp,
            EvaluationFlags flags)
        {
            var validAggregate = flags.Phase == QueryEvaluationPhase.Final &&
                                 (flags.Origin == ExpressionOrigin.Ordering ||
                                  flags.Origin == ExpressionOrigin.OutputColumn);
            if (call.CalledVariant.IsAggregate && !validAggregate)
            {
                throw new Exception("Illegal use of aggregate function here");
            }

            var arguments = new List<BaseValue>();
            if (call.CalledVariant.IsAggregate)
            {
                // Instead, we pass an identifier for each expression
                arguments.AddRange(
                    call.Arguments.Select(a => new TextValue(FormatTempRowReferenceAggFunc(call, a))));
            }
            else
            {
                // Evaluate the arguments
                foreach (var arg in call.Arguments)
                {
                    arguments.Add(EvaluateExpression(exec, arg, temp, flags));
                }
            }

            return call.Meta.WhenCalled(exec, arguments, call.CalledVariant.Identifier);
        }

        private static void PerformOperationOnStack(ExecutionContext exec, Operator op)
        {
            var right = exec.Stack.Pop();
            var left = exec.Stack.Pop();

            var tb = (bool val) => new BooleanValue(val);

            var result = op switch
            {
                Operator.Plus => left.Add(right),
                Operator.Minus => left.Subtract(right),
                Operator.ForwardSlash => left.Divide(right),
                Operator.Asterisk => left.Multiply(right),
                Operator.Remainder => left.Remainder(right),
                Operator.Power => left.Remainder(right),
                Operator.Equal => tb(left.Compare(right) == 0),
                Operator.NotEqual => tb(left.Compare(right) != 0),
                Operator.LessThan => tb(left.Compare(right) < 0),
                Operator.LessEqual => tb(left.Compare(right) <= 0),
                Operator.MoreThan => tb(left.Compare(right) > 0),
                Operator.MoreEqual => tb(left.Compare(right) >= 0),
                Operator.And => left.And(right),
                Operator.Or => left.Or(right),
                _ => throw new WoobyDatabaseException("Unreacheable code: Invalid operator")
            };
            
            exec.Stack.Push(result);
        }

        private void PerformSubSelect(ExecutionContext exec, SelectStatement subQuery)
        {
            var sub = new ExecutionContext(Context)
            {
                Previous = exec
            };
            ExecuteQuery(sub, subQuery);

            // Get first column of the last row in the target result
            if (sub.QueryOutput.Rows.Any())
            {
                var result = sub.QueryOutput.Rows.Last().Values[0];
                exec.Stack.Push(result);
            }
            else
            {
                exec.Stack.Push(new NullValue());
            }
        }

        private void PushNodeColumnValueToStack(ExecutionContext exec, Expression.Node node, TempRow? tempRow)
        {
            switch (node.Kind)
            {
                case Expression.NodeKind.Number:
                    exec.Stack.Push(new NumberValue(node.NumberValue));
                    break;
                case Expression.NodeKind.String:
                    exec.Stack.Push(new TextValue(node.StringValue));
                    break;
                case Expression.NodeKind.Null:
                    exec.Stack.Push(new NullValue());
                    break;
                case Expression.NodeKind.Reference:
                    if (tempRow == null ||
                        !tempRow.Value.EvaluatedReferences.TryGetValue(node.ReferenceValue.Join(), out BaseValue value))
                    {
                        value = ReadColumnReference(exec, node.ReferenceValue);
                    }

                    exec.Stack.Push(value);
                    break;
            }
        }

        private int EvaluateSubExpression(int offset, ExecutionContext exec, Expression expr, TempRow? tempRow,
            EvaluationFlags flags)
        {
            var opStack = new Stack<Operator>();
            var lastWasPrecedence = false;

            int i;
            for (i = offset; i < expr.Nodes.Count; ++i)
            {
                var node = expr.Nodes[i];

                if (lastWasPrecedence)
                {
                    if (node.Kind == Expression.NodeKind.Function)
                    {
                        var call = node.FunctionCall;
                        if (tempRow == null ||
                            !tempRow.Value.EvaluatedReferences.TryGetValue(call.FullText, out BaseValue value))
                        {
                            if (call.CalledVariant.IsAggregate)
                            {
                                if (flags.Origin == ExpressionOrigin.Grouping ||
                                    flags.Origin == ExpressionOrigin.Filter)
                                {
                                    throw new Exception("Invalid use of aggregate function");
                                }
                                else if (flags.Phase != QueryEvaluationPhase.Final)
                                {
                                    throw new Exception(
                                        "Internal error: Tried to evaluate aggregate function in caching phase");
                                }
                            }

                            value = EvaluateFunctionCall(exec, call, tempRow, flags);
                        }

                        exec.Stack.Push(value);
                    }
                    else if (node.Kind == Expression.NodeKind.SubSelect)
                    {
                        PerformSubSelect(exec, node.SubSelect);
                    }
                    else
                    {
                        PushNodeColumnValueToStack(exec, node, tempRow);
                    }

                    PerformOperationOnStack(exec, opStack.Pop());
                    lastWasPrecedence = false;
                }
                else if (node.Kind == Expression.NodeKind.Operator)
                {
                    if (node.OperatorValue == Operator.ParenthesisLeft)
                    {
                        i = EvaluateSubExpression(i + 1, exec, expr, tempRow, flags);
                    }
                    else if (node.OperatorValue == Operator.ParenthesisRight)
                    {
                        break;
                    }

                    if (OperatorIsBoolean(node.OperatorValue))
                    {
                        while (opStack.Count > 0 && !OperatorIsLowerBoolean(opStack.Last()))
                        {
                            PerformOperationOnStack(exec, opStack.Pop());
                        }
                    }

                    opStack.Push(node.OperatorValue);

                    switch (node.OperatorValue)
                    {
                        case Operator.Asterisk:
                        case Operator.ForwardSlash:
                        case Operator.Remainder:
                            lastWasPrecedence = true;
                            break;
                    }
                }
                else
                {
                    if (node.Kind == Expression.NodeKind.Function)
                    {
                        if (tempRow != null)
                        {
                            var call = node.FunctionCall;
                            if (!tempRow.Value.EvaluatedReferences.TryGetValue(call.FullText, out BaseValue value))
                            {
                                value = EvaluateFunctionCall(exec, call, tempRow, flags);
                            }

                            exec.Stack.Push(value);
                        }
                        else
                        {
                            throw new Exception("EvaluateSubExpression: TempRow is Null");
                        }
                    }
                    else if (node.Kind == Expression.NodeKind.SubSelect)
                    {
                        PerformSubSelect(exec, node.SubSelect);
                    }
                    else
                    {
                        PushNodeColumnValueToStack(exec, node, tempRow);
                    }
                }
            }

            while (opStack.Count > 0)
            {
                PerformOperationOnStack(exec, opStack.Pop());
            }

            return i;
        }

        private BaseValue EvaluateExpression(ExecutionContext exec, Expression expr, TempRow? temp,
            EvaluationFlags flags)
        {
            EvaluateSubExpression(0, exec, expr, temp, flags);
            return exec.PopStack();
        }

        private void EvaluateSingleReference(ExecutionContext exec, TempRow temp, ColumnReference reference)
        {
            if (!temp.EvaluatedReferences.ContainsKey(reference.Join()))
            {
                temp.EvaluatedReferences.Add(reference.Join(), ReadColumnReference(exec, reference));
            }
        }

        private static string FormatTempRowReferenceAggFunc(FunctionCall call, Expression param)
        {
            return $"_{call.CalledVariant.Identifier}_{param.FullText}";
        }

        private void EvaluateCurrentRowReferences(ExecutionContext exec, Expression expr, TempRow temp,
            EvaluationFlags flags)
        {
            foreach (var node in expr.Nodes)
            {
                if (node.Kind == Expression.NodeKind.Reference)
                {
                    EvaluateSingleReference(exec, temp, node.ReferenceValue);
                }
                else if (node.Kind == Expression.NodeKind.Function)
                {
                    var call = node.FunctionCall;
                    if (temp.EvaluatedReferences.ContainsKey(call.FullText))
                    {
                        continue;
                    }

                    if (call.CalledVariant.IsAggregate)
                    {
                        foreach (var param in call.Arguments)
                        {
                            temp.EvaluatedReferences.Add(FormatTempRowReferenceAggFunc(call, param),
                                EvaluateExpression(exec, param, temp, flags));
                        }
                    }
                    else
                    {
                        temp.EvaluatedReferences.Add(call.FullText, EvaluateFunctionCall(exec, call, temp, flags));
                    }
                }
            }
        }
        
        private bool RecursiveSeekQuery(ExecutionContext exec, SelectStatement query, int idx, bool reset = false)
        {
            Expression condition;
            bool rowIsOptional;
            if (idx == 0)
            {
                condition = query.FilterConditions;
                rowIsOptional = false;
            }
            else
            {
                condition = query.Joinings[idx - 1].Condition;
                rowIsOptional = query.Joinings[idx - 1].Kind == JoinKind.Left; // FIXME
            }

            bool success = true;

            var flags = new EvaluationFlags {Origin = ExpressionOrigin.Filter, Phase = QueryEvaluationPhase.Final};
            var source = exec.Sources[idx];
            source.Matched = true;

            var loopUntilRowMatches = () =>
            {
                do
                {
                    if (!source.DataProvider.SeekNext())
                    {
                        source.Matched = false;
                        return false;
                    }
                    
                    if (condition != null)
                    {
                        var filter = EvaluateExpression(exec, condition, null, flags);

                        if (filter is BooleanValue filterBoolean)
                        {
                            if (filterBoolean.Value)
                            {
                                source.Matched = filterBoolean.Value;
                                return true;
                            }
                        }
                        else
                        {
                            throw new Exception("Expected boolean result for WHERE clause expression");
                        }
                    }
                    else
                    {
                        source.Matched = true;
                        return true;
                    }
                } while (true);
            };

            bool shouldSeek = true;

            if (source.DataProvider.CurrentRowId() < 0 || reset)
            {
                // Not initialized or need to be reset
                source.DataProvider.SeekFirst();
                if (!loopUntilRowMatches())
                {
                    success = rowIsOptional;
                }

                source.LastMatched = false;
                shouldSeek = false;
            }

            if (success)
            {
                if (idx < exec.Sources.Count - 1)
                {
                    do
                    {
                        if (RecursiveSeekQuery(exec, query, idx + 1, reset))
                        {
                            // We found a row in the next (or one of the next) sources, so just continue and read from it
                            break;
                        }

                        // Since the row is optional and we couldn't find a matching row, add an empty one for the time being
                        if (rowIsOptional && !source.LastMatched)
                        {
                            break;
                        }
                        
                        // Couldn't find a row, so we need to seek this one and try again
                        if (loopUntilRowMatches())
                        {
                            // Try to seek the next sources again
                            reset = true;
                            continue;
                        }

                        break;
                    } while (true);
                }
                else if(shouldSeek)
                {
                    loopUntilRowMatches();
                }
            }

            if (!source.Matched)
            {
                success = rowIsOptional && !source.LastMatched;
                source.LastMatched = success;
            }
            else
            {
                source.LastMatched = true;
            }

            return success;
        }

        private void ExecuteQuery(ExecutionContext exec, SelectStatement query)
        {
            SetupSources(exec, query);
            exec.ResetRowNumber();

            foreach (var output in query.OutputColumns)
            {
                PrepareQueryOutput(exec, output);
            }

            // Gather required info for all rows that match the filter
            // Seek the last row in the join until none can be found for the given filter, then trace back until we seek the main source
            while (RecursiveSeekQuery(exec, query, 0))
            {
                var flags = new EvaluationFlags {Phase = QueryEvaluationPhase.Caching};
                // Row passed on filter
                var tempRow = exec.CreateTempRow();
                flags.Origin = ExpressionOrigin.OutputColumn;
                foreach (var output in query.OutputColumns)
                {
                    EvaluateCurrentRowReferences(exec, output, tempRow, flags);
                }

                flags.Origin = ExpressionOrigin.Ordering;
                foreach (var ordering in query.OutputOrder)
                {
                    EvaluateCurrentRowReferences(exec, ordering.OrderExpression, tempRow, flags);
                }

                foreach (var grouping in query.Grouping)
                {
                    EvaluateCurrentRowReferences(exec, grouping, tempRow, flags);
                }

                exec.TempRows.Add(tempRow);
                exec.IncrementRowNumber();
            }

            exec.ResetRowNumber();

            GroupRows(exec, query);
            DistinctRows(exec, query);
            OrderOutputRows(exec, query);
            //PushAllRowsToOutput(exec, query);
            // Final check so we don't return an empty row when no rows were found
            CheckOutputRows(exec);
        }

        private void SetupSources(ExecutionContext exec, Statement statement)
        {
            SetupMainSource(exec, statement.MainSource);
            if (statement is SelectStatement query)
            {
                foreach (var join in query.Joinings)
                {
                    SetupMainSource(exec, join.Source);
                }
            }
        }

        private void SetupMainSource(ExecutionContext exec, TableSource source)
        {
            TableMeta sourceData;
            if (source.Kind == TableSource.SourceKind.Reference)
            {
                sourceData = Context.FindTable(source.Reference);
            }
            else
            {
                sourceData = source.BuildMetaFromSubSelect(exec.Context);
                // Execute sub select passed as the main source
                var subExec = new ExecutionContext(exec.Context)
                {
                    Previous = exec
                };
                ExecuteQuery(subExec, source.SubSelect);
                // Populate temp table
                if (sourceData.DataProvider is InMemoryDataProvider provider)
                {
                    provider.SetupRows(subExec.QueryOutput.Rows.Select(r => r.Values));
                }
                else throw new WoobyDatabaseException("Internal error: Invalid provider for temporary query table");
            }
            
            exec.Sources.Add(new ExecutionDataSource()
            {
                Meta = sourceData,
                DataProvider = new TableCursor(sourceData.DataProvider, sourceData.Columns.Count),
                Alias = source.Identifier
            });
        }

        public ExecutionContext Execute(Statement statement)
        {
            var exec = new ExecutionContext(Context);

            if (statement is SelectStatement query)
            {
                ExecuteQuery(exec, query);
            }
            else if (statement is CreateStatement create)
            {
                ExecuteCreate(exec, create);
            }
            else if (statement is InsertStatement insert)
            {
                ExecuteInsert(exec, insert);
            }
            else if (statement is UpdateStatement update)
            {
                ExecuteUpdate(exec, update);
            }
            else if (statement is DeleteStatement delete)
            {
                ExecuteDelete(exec, delete);
            }

            return exec;
        }

        private ExecutionContext GetContextForLevel(ExecutionContext root, int level)
        {
            if (level <= 0)
            {
                return root;
            }

            if (root.Previous == null)
            {
                throw new IndexOutOfRangeException(
                    "Given level surpasses context availability in the current execution");
            }

            return GetContextForLevel(root.Previous, level - 1);
        }
    }
}