using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

using wooby.Database.Persistence;
using wooby.Parsing;

namespace wooby.Database
{
    public class Machine
    {
        private List<Function> Functions { get; set; }
        private List<TableData> Tables { get; set; }
        public Context Context { get; private set; } = null;
        private static readonly List<Operator> BooleanOperators = new List<Operator> { Operator.Equal, Operator.NotEqual, Operator.LessEqual, Operator.MoreEqual, Operator.MoreThan, Operator.LessThan };

        private static bool OperatorIsBoolean(Operator op)
        {
            return BooleanOperators.Contains(op);
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
            long id = 0;

            Functions = new List<Function>()
            {
                new CurrentDate_Function(id++),
                new DatabaseName_Function(id++),
                new RowNum_Function(id++),
                new RowId_Function(id++),
                new Trunc_Function(id++),
                new Count_Function(id++),
            };

            foreach (var func in Functions)
            {
                Context.AddFunction(func);
            }
        }

        private void InitializeTables()
        {
            Tables = new List<TableData>();

            Context.Tables.RemoveAll(table => !table.IsReal);

            foreach (var table in Context.Tables)
            {
                RegisterTable(table, PersistenceBackendHelper.GetTableDataProvider(Context));
            }

            var dualMeta = new TableMeta()
            {
                Name = "dual",
                Columns = new List<ColumnMeta>(),
                IsReal = false,
                IsTemporary = false
            };
            RegisterTable(dualMeta, new Dual_DataProvider());

            var loveliveMeta = new TableMeta() { Name = "lovelive", IsReal = false }
                .AddColumn("id", ColumnType.Number)
                .AddColumn("parent_id", ColumnType.Number)
                .AddColumn("nome", ColumnType.String)
                .AddColumn("ano", ColumnType.Number)
                .AddColumn("integrantes", ColumnType.Number);
            RegisterTable(loveliveMeta, new LoveLive_DataProvider());
        }

        private void RegisterTable(TableMeta Meta, ITableDataProvider Provider)
        {
            if (Context.Tables.Find(t => t.Name == Meta.Name) == null)
            {
                Context.AddTable(Meta);
            }
            Provider.Initialize(Context, Meta);
            Tables.Add(new TableData { Meta = Meta, DataProvider = Provider });
        }

        private static void CheckOutputRows(ExecutionContext context)
        {
            if (context.QueryOutput.Rows.Count == 1 && context.QueryOutput.Rows[0].All(v => v.Kind == ValueKind.Null))
            {
                context.QueryOutput.Rows.RemoveAt(0);
            }
        }

        private static void PushToOutput(ExecutionContext context, ColumnValue value)
        {
            if (context.QueryOutput.Rows.Count == 0)
            {
                throw new InvalidOperationException("Query output has no rows to push to");
            }

            context.QueryOutput.Rows.Last().Add(value);
        }

        private static void PushToGrouping(ExecutionContext context, ColumnValue value)
        {
            if (context.GroupingResults.Count == 0)
            {
                throw new InvalidOperationException("Query ordering has no rows to push to");
            }

            context.GroupingResults.Last().Values.Add(value);
        }

        private static void PushToOrdering(ExecutionContext context, ColumnValue value)
        {
            if (context.OrderingResults.Count == 0)
            {
                throw new InvalidOperationException("Query ordering has no rows to push to");
            }

            context.OrderingResults.Last().Values.Add(value);
        }

        public class ColumnValueComparer : IComparer<ColumnValue>, IEqualityComparer<ColumnValue>
        {
            public int Compare(ColumnValue x, ColumnValue y)
            {
                if (IsGreater(x, y, false, true).Boolean)
                {
                    return 1;
                }
                else
                {
                    return Equal(x, y).Boolean ? 0 : -1;
                }
            }

            public bool Equals(ColumnValue x, ColumnValue y)
            {
                return x.Kind == y.Kind && Equal(x, y).Boolean;
            }

            public int GetHashCode([DisallowNull] ColumnValue obj)
            {
                return base.GetHashCode();
            }
        }

        private static void AssertValuesNotBoolean(ColumnValue a)
        {
            if (a.Kind == ValueKind.Boolean)
            {
                throw new ArgumentException("One of the arguments to the expression is a boolean value");
            }
        }

        private static void AssertValuesNotBoolean(ColumnValue a, ColumnValue b)
        {
            AssertValuesNotBoolean(a);
            AssertValuesNotBoolean(b);
        }

        private static bool AnyValuesNull(ColumnValue a, ColumnValue b)
        {
            return a.Kind == ValueKind.Null || b.Kind == ValueKind.Null;
        }

        private static ColumnValue Sum(ExecutionContext context)
        {
            var right = context.Stack.Pop();
            var left = context.Stack.Pop();

            AssertValuesNotBoolean(left, right);
            if (AnyValuesNull(left, right))
            {
                return ColumnValue.Null();
            }

            if (left.Kind == ValueKind.Number)
            {
                var lnum = left.Number;
                var rnum = right.Number;

                return new ColumnValue() { Number = lnum + rnum, Kind = ValueKind.Number };
            }
            else if (left.Kind == ValueKind.Text)
            {
                var lstr = left.Text;
                var rstr = right.Text;
                return new ColumnValue() { Text = lstr + rstr, Kind = ValueKind.Text };
            }

            throw new ArgumentException("Incompatible values for sum operation");
        }


        private static ColumnValue Equal(ColumnValue left, ColumnValue right)
        {
            AssertValuesNotBoolean(left, right);
            if (AnyValuesNull(left, right))
            {
                var equal = AnyValuesNull(left, left) && AnyValuesNull(right, right);
                return new ColumnValue() { Kind = ValueKind.Boolean, Boolean = equal };
            }

            if (left.Kind == ValueKind.Number)
            {
                var lnum = left.Number;
                var rnum = right.Number;

                return new ColumnValue() { Boolean = lnum == rnum, Kind = ValueKind.Boolean };
            }
            else if (left.Kind == ValueKind.Text)
            {
                var lstr = left.Text;
                var rstr = right.Text;
                return new ColumnValue() { Boolean = lstr == rstr, Kind = ValueKind.Boolean };
            }

            throw new ArgumentException("Incompatible values for equals operation");
        }

        private static ColumnValue Equal(ExecutionContext context)
        {
            var right = context.Stack.Pop();
            var left = context.Stack.Pop();

            return Equal(left, right);
        }

        private static ColumnValue NotEqual(ExecutionContext context)
        {
            var result = Equal(context);

            if (result.Kind == ValueKind.Boolean)
            {
                result.Boolean = !result.Boolean;
            }
            else
            {
                result.Kind = ValueKind.Boolean;
                result.Boolean = true;
            }

            return result;
        }

        private static ColumnValue Less(ExecutionContext context, bool orEqual)
        {
            var right = context.Stack.Pop();
            var left = context.Stack.Pop();

            AssertValuesNotBoolean(left, right);
            if (AnyValuesNull(left, right))
            {
                return new ColumnValue { Kind = ValueKind.Boolean, Boolean = false };
            }

            var result = new ColumnValue() { Boolean = false, Kind = ValueKind.Boolean };

            if (left.Kind == ValueKind.Number)
            {
                var lnum = left.Number;
                var rnum = right.Number;

                result.Boolean = lnum < rnum;
            }
            else if (left.Kind == ValueKind.Text)
            {
                throw new ArgumentException("Invalid operation between strings");
            }

            if (!result.Boolean && orEqual)
            {
                result = Equal(left, right);
            }

            return result;
        }

        private static ColumnValue IsGreater(ColumnValue left, ColumnValue right, bool orEqual, bool isOrdering = false)
        {
            AssertValuesNotBoolean(left, right);
            if (AnyValuesNull(left, right))
            {
                return new ColumnValue { Kind = ValueKind.Boolean, Boolean = false };
            }

            var result = new ColumnValue() { Boolean = false, Kind = ValueKind.Boolean };

            if (left.Kind == ValueKind.Number)
            {
                var lnum = left.Number;
                var rnum = right.Number;

                result.Boolean = lnum > rnum;
            }
            else if (left.Kind == ValueKind.Text)
            {
                if (isOrdering)
                {
                    result.Boolean = string.Compare(left.Text, right.Text) > 0;
                }
                else
                {
                    throw new ArgumentException("Invalid operation between strings");
                }
            }

            if (!result.Boolean && orEqual)
            {
                result = Equal(left, right);
            }

            return result;
        }

        private static ColumnValue Greater(ExecutionContext context, bool orEqual)
        {
            var right = context.Stack.Pop();
            var left = context.Stack.Pop();

            return IsGreater(left, right, orEqual);
        }

        private static ColumnValue Divide(ExecutionContext context)
        {
            var right = context.Stack.Pop();
            var left = context.Stack.Pop();

            AssertValuesNotBoolean(left, right);
            if (AnyValuesNull(left, right))
            {
                return ColumnValue.Null();
            }

            if (left.Kind == ValueKind.Number)
            {
                var lnum = left.Number;
                var rnum = right.Number;

                return new ColumnValue() { Number = lnum / rnum, Kind = ValueKind.Number };
            }
            else if (left.Kind == ValueKind.Text)
            {
                throw new Exception("Invalid operation between strings");
            }

            throw new ArgumentException("Invalid arguments for division");
        }

        private static ColumnValue Remainder(ExecutionContext context)
        {
            var right = context.Stack.Pop();
            var left = context.Stack.Pop();

            AssertValuesNotBoolean(left, right);
            if (AnyValuesNull(left, right))
            {
                return ColumnValue.Null();
            }

            if (left.Kind == ValueKind.Number)
            {
                var lnum = left.Number;
                var rnum = right.Number;

                return new ColumnValue() { Number = lnum % rnum, Kind = ValueKind.Number };
            }
            else if (left.Kind == ValueKind.Text)
            {
                throw new Exception("Invalid operation between strings");
            }

            throw new ArgumentException("Invalid arguments for division");
        }

        private static ColumnValue Multiply(ExecutionContext context)
        {
            var right = context.Stack.Pop();
            var left = context.Stack.Pop();

            AssertValuesNotBoolean(left, right);
            if (AnyValuesNull(left, right))
            {
                return ColumnValue.Null();
            }

            if (left.Kind == ValueKind.Number)
            {
                var lnum = left.Number;
                var rnum = right.Number;

                return new ColumnValue() { Number = lnum * rnum, Kind = ValueKind.Number };
            }
            else if (left.Kind == ValueKind.Text)
            {
                throw new ArgumentException("Invalid operation between strings");
            }

            throw new ArgumentException("Invalid arguments for multiplication");
        }

        private static ColumnValue Sub(ExecutionContext context)
        {
            var right = context.Stack.Pop();
            var left = context.Stack.Pop();

            AssertValuesNotBoolean(left, right);
            if (AnyValuesNull(left, right))
            {
                return ColumnValue.Null();
            }

            if (left.Kind == ValueKind.Number)
            {
                var lnum = left.Number;
                var rnum = right.Number;

                return new ColumnValue() { Number = lnum - rnum, Kind = ValueKind.Number };
            }
            else if (left.Kind == ValueKind.Text)
            {
                throw new Exception("Invalid operation between strings");
            }

            throw new ArgumentException("Invalid arguments provided for subtraction");
        }

        private void PrepareQueryOutput(ExecutionContext exec, SelectStatement query, Expression expr)
        {
            var id = expr.Identifier;
            if (string.IsNullOrEmpty(id))
            {
                id = expr.FullText;
            }

            exec.QueryOutput.Definition.Add(new OutputColumnMeta() { OutputName = id, Visible = true });
        }

        private static List<RowOrderingIntermediate> BuildFromRows(ExecutionContext exec, SelectStatement query, List<int> indexes, int colIndex)
        {
            var result = new List<RowOrderingIntermediate>();
            var ascending = query.OutputOrder[colIndex].Kind == OrderingKind.Ascending;

            IEnumerable<RowMetaData> input;
            if (indexes != null && indexes.Count > 0)
            {
                input = exec.OrderingResults.Where(r => indexes.Contains(r.RowIndex));
            }
            else
            {
                input = exec.OrderingResults.AsEnumerable();
            }

            var groups = input.GroupBy(row => row.Values[colIndex], new ColumnValueComparer()).OrderBy(r => r.Key, new ColumnValueComparer());
            if (ascending)
            {
                foreach (var group in groups)
                {
                    result.Add(new RowOrderingIntermediate()
                    {
                        DistinctValue = group.Key,
                        MatchingRows = group.Select(row => row.RowIndex).ToList()
                    });
                }
            }
            else
            {
                foreach (var group in groups.Reverse())
                {
                    result.Add(new RowOrderingIntermediate()
                    {
                        DistinctValue = group.Key,
                        MatchingRows = group.Select(row => row.RowIndex).ToList()
                    });
                }
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

                var newResult = new List<List<ColumnValue>>(exec.QueryOutput.Rows.Count);
                foreach (var item in group)
                {
                    item.Collect(exec, newResult);
                }

                exec.QueryOutput.Rows = newResult;
            }
        }

        private List<List<TempRow>> GroupRowsRecursive(ExecutionContext exec, List<TempRow> current, int level, SelectStatement query)
        {
            var value = query.Grouping[level].Join();
            var groups = new Dictionary<string, List<TempRow>>();

            foreach (var row in current)
            {
                var correspondingResult = row.EvaluatedReferences[value];
                var distinct = correspondingResult.PrettyPrint();
                if (groups.TryGetValue(distinct, out var sub))
                {
                    sub.Add(row);
                }
                else
                {
                    groups.Add(distinct, new List<TempRow> { row });
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

            var flags = new EvaluationFlags { Origin = ExpressionOrigin.Ordering, Phase = QueryEvaluationPhase.Final };
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

        private void GroupRows(ExecutionContext exec, SelectStatement query)
        {
            exec.ResetRowNumber();
            var flags = new EvaluationFlags { Origin = ExpressionOrigin.OutputColumn, Phase = QueryEvaluationPhase.Final };

            // Reset since we're gonna read from it again if needed
            exec.MainSource.DataProvider.Seek(0);
            // For simplicity, we only allow for grouping by a column name for now
            // TODO: Grouping by expression
            // Should be an easy enough fix
            if (query.Grouping.Count > 0)
            {
                var originalTempRows = exec.TempRows;
                var groups = GroupRowsRecursive(exec, originalTempRows, 0, query);
                foreach (var group in groups)
                {
                    exec.MainSource.DataProvider.SeekNext();
                    exec.TempRows = group;

                    var row = new List<ColumnValue>();
                    // For each sub group, now generate one output row
                    foreach (var expr in query.OutputColumns)
                    {
                        exec.Stack.Clear();
                        row.Add(EvaluateExpression(exec, expr, group[0], flags));
                        while (exec.Stack.Count > 0)
                        {
                            row.Add(exec.PopStack());
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
                    exec.MainSource.DataProvider.SeekNext();
                    var r = new List<ColumnValue>();
                    // For each sub group, now generate one output row
                    foreach (var expr in query.OutputColumns)
                    {
                        exec.Stack.Clear();
                        r.Add(EvaluateExpression(exec, expr, temp, flags));
                        while (exec.Stack.Count > 0)
                        {
                            r.Add(exec.PopStack());
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
            var newColumns = new Dictionary<int, ColumnValue>();
            var table = exec.Context.FindTable(insert.MainSource);
            if (table == null)
            {
                throw new Exception("Could not find table");
            }

            if (insert.Columns.Count != 0 && insert.Columns.Count != insert.Values.Count || (insert.Columns.Count == 0 && table.Columns.Count != insert.Values.Count))
            {
                throw new Exception("Error: Different length for Columns list as Values list");
            }

            var numCols = insert.Columns.Count == 0 ? table.Columns.Count : insert.Columns.Count;

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

                var instructions = new List<Instruction>();
                Compiler.CompileExpression(insert, insert.Values[i], exec.Context, instructions, PushResultKind.None);
                Execute(instructions, exec);

                newColumns.Add(idx, exec.PopStack());
            }

            SetupMainSource(exec, insert.MainSource);

            exec.MainSource.DataProvider.Insert(newColumns);
            exec.RowsAffected = 1;
        }

        private void ExecuteUpdate(ExecutionContext exec, UpdateStatement update)
        {
            var filter = new List<Instruction>();
            if (update.FilterConditions != null)
            {
                Compiler.CompileExpression(update, update.FilterConditions, Context, filter, PushResultKind.None);
            }

            SetupMainSource(exec, update.MainSource);
            int affected = 0;

            // Important: The instructions are to be executed every line to compute new results based on (possibly) the current values
            var updateColumns = new List<int>();
            var updateInstructions = new List<Instruction>();

            foreach (var col in update.Columns)
            {
                var column = exec.Context.FindColumn(col.Item1);

                if (column == null)
                {
                    throw new Exception("Could not find referenced column");
                }

                updateColumns.Add(column.Id);

                Compiler.CompileExpression(update, col.Item2, Context, updateInstructions, PushResultKind.ToOutput);
            }

            exec.QueryOutput.Rows.Add(new List<ColumnValue>());
            while (exec.MainSource.DataProvider.SeekNext())
            {
                if (FilterIsTrueForCurrentRow(exec, filter))
                {
                    Execute(updateInstructions, exec);
                    // In order, perform the update utilizing the columns in the output

                    var dict = new Dictionary<int, ColumnValue>();

                    int outputIndex = 0;
                    foreach (var index in updateColumns)
                    {
                        dict.Add(index, exec.QueryOutput.Rows[0][outputIndex++]);
                    }

                    exec.MainSource.DataProvider.Update(dict);

                    // Clear for the next row
                    exec.QueryOutput.Rows[0].Clear();
                    affected += 1;
                }
            }

            exec.RowsAffected = affected;
        }

        private void ExecuteDelete(ExecutionContext exec, DeleteStatement delete)
        {
            var filter = new List<Instruction>();
            if (delete.FilterConditions != null)
            {
                Compiler.CompileExpression(delete, delete.FilterConditions, Context, filter, PushResultKind.None);
            }

            SetupMainSource(exec, delete.MainSource);
            int affected = 0;

            while (exec.MainSource.DataProvider.SeekNext())
            {
                if (FilterIsTrueForCurrentRow(exec, filter))
                {
                    affected += 1;
                    exec.MainSource.DataProvider.Delete();
                }
            }

            exec.RowsAffected = affected;
        }

        private ColumnValue ReadColumnReference(ExecutionContext exec, ColumnReference reference)
        {
            var sourceContext = GetContextForLevel(exec, reference.ParentLevel);
            if (sourceContext.RowNumber > sourceContext.TempRows.Count - 1 || !sourceContext.TempRows[sourceContext.RowNumber].EvaluatedReferences.TryGetValue(reference.Join(), out ColumnValue value))
            {
                var meta = exec.Context.FindColumn(reference);
                value = sourceContext.MainSource.DataProvider.Read(meta.Id);
            }
            return value;
        }

        private ColumnValue EvaluateFunctionCall(ExecutionContext exec, FunctionCall call, TempRow temp, EvaluationFlags flags)
        {
            var validAggregate = flags.Phase == QueryEvaluationPhase.Final &&
                (flags.Origin == ExpressionOrigin.Ordering || flags.Origin == ExpressionOrigin.OutputColumn);
            var f = exec.Context.FindFunction(call.Name);
            if (f.IsAggregate && !validAggregate)
            {
                throw new Exception("Illegal use of aggregate function here");
            }

            var arguments = new List<ColumnValue>();
            // Evaluate the arguments
            foreach (var arg in call.Arguments)
            {
                arguments.Add(EvaluateExpression(exec, arg, temp, flags));
            }

            return Functions.Find(fu => fu.Id == f.Id).WhenCalled(exec, arguments);
        }

        private static void PerformOperationOnStack(ExecutionContext exec, Operator op)
        {
            switch (op)
            {
                case Operator.Plus:
                    exec.Stack.Push(Sum(exec));
                    break;
                case Operator.Minus:
                    exec.Stack.Push(Sub(exec));
                    break;
                case Operator.ForwardSlash:
                    exec.Stack.Push(Divide(exec));
                    break;
                case Operator.Remainder:
                    exec.Stack.Push(Remainder(exec));
                    break;
                case Operator.Asterisk:
                    exec.Stack.Push(Multiply(exec));
                    break;
                case Operator.Equal:
                    exec.Stack.Push(Equal(exec));
                    break;
                case Operator.NotEqual:
                    exec.Stack.Push(NotEqual(exec));
                    break;
                case Operator.LessThan:
                    exec.Stack.Push(Less(exec, false));
                    break;
                case Operator.MoreThan:
                    exec.Stack.Push(Greater(exec, false));
                    break;
                case Operator.LessEqual:
                    exec.Stack.Push(Less(exec, true));
                    break;
                case Operator.MoreEqual:
                    exec.Stack.Push(Greater(exec, true));
                    break;
            }
        }

        private void PerformSubSelect(ExecutionContext exec, SelectStatement subQuery)
        {
            var sub = new ExecutionContext(Context)
            {
                Previous = exec
            };
            ExecuteQuery(sub, subQuery);

            // Get first column of the last row in the target result
            if (sub.QueryOutput.Rows.Count > 0)
            {
                var result = sub.QueryOutput.Rows.Last()[0];
                exec.Stack.Push(result);
            }
            else
            {
                exec.Stack.Push(new ColumnValue { Kind = ValueKind.Null });
            }
        }

        private void PushNodeColumnValueToStack(ExecutionContext exec, Expression.Node node, TempRow tempRow)
        {
            switch (node.Kind)
            {
                case Expression.NodeKind.Number:
                    exec.Stack.Push(new ColumnValue { Kind = ValueKind.Number, Number = node.NumberValue });
                    break;
                case Expression.NodeKind.String:
                    exec.Stack.Push(new ColumnValue { Kind = ValueKind.Text, Text = node.StringValue });
                    break;
                case Expression.NodeKind.Reference:
                    if (tempRow == null || !tempRow.EvaluatedReferences.TryGetValue(node.ReferenceValue.Join(), out ColumnValue value))
                    {
                        value = ReadColumnReference(exec, node.ReferenceValue);
                    }
                    exec.Stack.Push(value);
                    break;
            }
        }

        private int EvaluateSubExpression(int offset, ExecutionContext exec, Expression expr, TempRow tempRow, EvaluationFlags flags)
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
                        if (tempRow == null || !tempRow.EvaluatedReferences.TryGetValue(call.FullText, out ColumnValue value))
                        {
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
                        while (opStack.Count > 0)
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
                        default:
                            break;
                    }
                }
                else
                {
                    if (node.Kind == Expression.NodeKind.Function)
                    {
                        var call = node.FunctionCall;
                        if (!tempRow.EvaluatedReferences.TryGetValue(call.FullText, out ColumnValue value))
                        {
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
                }
            }

            while (opStack.Count > 0)
            {
                PerformOperationOnStack(exec, opStack.Pop());
            }

            return i;
        }

        private ColumnValue EvaluateExpression(ExecutionContext exec, Expression expr, TempRow temp, EvaluationFlags flags)
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

        private void EvaluateCurrentRowReferences(ExecutionContext exec, Expression expr, TempRow temp, EvaluationFlags flags)
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
                    if (call.IsAggregate || temp.EvaluatedReferences.ContainsKey(call.FullText))
                    {
                        continue;
                    }

                    temp.EvaluatedReferences.Add(call.FullText, EvaluateFunctionCall(exec, call, temp, flags));
                }
            }
        }

        private void ExecuteQuery(ExecutionContext exec, SelectStatement query)
        {
            SetupMainSource(exec, query.MainSource);

            foreach (var output in query.OutputColumns)
            {
                PrepareQueryOutput(exec, query, output);
            }

            // Gather required info for all rows that match the filter
            while (exec.MainSource.DataProvider.SeekNext())
            {
                if (query.FilterConditions != null)
                {
                    var filter = EvaluateExpression(exec, query.FilterConditions, null, new EvaluationFlags { Origin = ExpressionOrigin.Filter, Phase = QueryEvaluationPhase.Final });
                    if (filter.Kind != ValueKind.Boolean)
                    {
                        throw new Exception("Expected boolean result for WHERE clause expression");
                    }

                    if (!filter.Boolean)
                    {
                        continue;
                    }
                }

                var flags = new EvaluationFlags { Phase = QueryEvaluationPhase.Caching };
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
                    EvaluateSingleReference(exec, tempRow, grouping);
                }

                exec.TempRows.Add(tempRow);
                exec.IncrementRowNumber();
            }
            exec.ResetRowNumber();

            GroupRows(exec, query);
            OrderOutputRows(exec, query);
            //PushAllRowsToOutput(exec, query);
            // Final check so we don't return an empty row when no rows were found
            CheckOutputRows(exec);
        }

        private void OldExecuteQuery(ExecutionContext exec, SelectStatement query)
        {
            // Compile all expressions
            var outputExpressions = new List<Instruction>();

            foreach (var output in query.OutputColumns)
            {
                Compiler.CompileExpression(query, output, Context, outputExpressions, PushResultKind.ToOutput);
                PrepareQueryOutput(exec, query, output);
            }

            var filter = new List<Instruction>();
            if (query.FilterConditions != null)
            {
                Compiler.CompileExpression(query, query.FilterConditions, Context, filter, PushResultKind.None);
            }

            var ordering = new List<Instruction>();
            foreach (var ord in query.OutputOrder)
            {
                Compiler.CompileExpression(query, ord.OrderExpression, Context, ordering, PushResultKind.ToOrdering);
            }

            var grouping = new List<Instruction>();
            foreach (var group in query.Grouping)
            {
                Compiler.CompileExpression(query, new Expression { Nodes = { new Expression.Node { Kind = Expression.NodeKind.Reference, ReferenceValue = group } } }, Context, grouping, PushResultKind.ToGrouping);
            }

            SetupMainSource(exec, query.MainSource);

            // First, filter all columns in the source if a filter was specified
            var filteredRows = new List<long>();

            if (query.FilterConditions != null)
            {
                while (exec.MainSource.DataProvider.SeekNext())
                {
                    if (FilterIsTrueForCurrentRow(exec, filter))
                    {
                        exec.QueryOutput.Rows.Add(new List<ColumnValue>());
                        filteredRows.Add(exec.MainSource.DataProvider.CurrentRowId());
                    }
                };

                exec.QueryOutput.Rows.Clear();
            }

            var filterIndex = 0;

            while (true)
            {
                if (query.FilterConditions != null)
                {
                    if (filterIndex >= filteredRows.Count)
                    {
                        break;
                    }
                    else
                    {
                        exec.MainSource.DataProvider.Seek(filteredRows[filterIndex++]);
                    }
                }
                else if (!exec.MainSource.DataProvider.SeekNext())
                {
                    break;
                }

                var rowIndex = exec.QueryOutput.Rows.Count;
                // First get the actual outputs from the query
                exec.QueryOutput.Rows.Add(new List<ColumnValue>());
                Execute(outputExpressions, exec);
                // Fetch the required results for the ordering
                exec.OrderingResults.Add(new RowMetaData() { RowIndex = rowIndex });
                Execute(ordering, exec);
                // Fetch results to grouping
                exec.GroupingResults.Add(new RowMetaData() { RowIndex = rowIndex });
                Execute(grouping, exec);
            }

            // Perform ordering on output rows
            OrderOutputRows(exec, query);
            // Rebuild the output rows considering the grouping
            GroupRows(exec, query);
            // Final check so we don't return an empty row when no rows were found
            CheckOutputRows(exec);
        }

        private void SetupMainSource(ExecutionContext exec, long tableId)
        {
            var sourceData = Tables.Find(t => t.Meta.Id == tableId);
            exec.MainSource = new ExecutionDataSource()
            {
                Meta = sourceData.Meta,
                DataProvider = new TableCursor(sourceData.DataProvider, sourceData.Meta.Columns.Count)
            };
        }

        private void SetupMainSource(ExecutionContext exec, ColumnReference source)
        {
            SetupMainSource(exec, Context.FindTable(source).Id);
        }

        private bool FilterIsTrueForCurrentRow(ExecutionContext exec, List<Instruction> filter)
        {
            if (filter.Count == 0)
            {
                return true;
            }

            Execute(filter, exec);

            var top = exec.PopStack(true);
            return filter.Count == 0 || (top != null && top.Kind == ValueKind.Boolean && top.Boolean);
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

        private static List<ColumnValue> PopFunctionArguments(ExecutionContext exec, int numArgs)
        {
            var result = new List<ColumnValue>(numArgs);

            for (int i = 0; i < numArgs; ++i)
            {
                result.Add(exec.Stack.Pop());
            }

            result.Reverse();
            return result;
        }

        private ExecutionContext GetContextForLevel(ExecutionContext root, int level)
        {
            if (level <= 0)
            {
                return root;
            }

            if (root.Previous == null)
            {
                throw new IndexOutOfRangeException("Given level surpasses context availability in the current execution");
            }

            return GetContextForLevel(root.Previous, level - 1);
        }

        public void Execute(List<Instruction> instructions, ExecutionContext exec)
        {
            for (int i = 0; i < instructions.Count; ++i)
            {
                var instruction = instructions[i];
                ExecutionContext sourceContext = null;

                switch (instruction.OpCode)
                {
                    case OpCode.PushColumnToOutput:
                        sourceContext = GetContextForLevel(exec, (int)instruction.Arg3);
                        PushToOutput(exec, sourceContext.MainSource.DataProvider.Read((int)instruction.Arg2));
                        break;
                    case OpCode.CallFunction:
                        var numArgs = (int)instruction.Arg2;
                        if (numArgs > exec.Stack.Count)
                        {
                            throw new InvalidOperationException("Expected arguments for function call do not match the stack contents");
                        }
                        exec.Stack.Push(Functions.Find(v => v.Id == instruction.Arg1).WhenCalled(exec, PopFunctionArguments(exec, numArgs)));
                        break;
                    case OpCode.PushNumber:
                        exec.Stack.Push(new ColumnValue() { Kind = ValueKind.Number, Number = instruction.Num1 });
                        break;
                    case OpCode.PushString:
                        exec.Stack.Push(new ColumnValue() { Kind = ValueKind.Text, Text = instruction.Str1 });
                        break;
                    case OpCode.Sum:
                        exec.Stack.Push(Sum(exec));
                        break;
                    case OpCode.Sub:
                        exec.Stack.Push(Sub(exec));
                        break;
                    case OpCode.Div:
                        exec.Stack.Push(Divide(exec));
                        break;
                    case OpCode.Rem:
                        exec.Stack.Push(Remainder(exec));
                        break;
                    case OpCode.Mul:
                        exec.Stack.Push(Multiply(exec));
                        break;
                    case OpCode.Eq:
                        exec.Stack.Push(Equal(exec));
                        break;
                    case OpCode.NEq:
                        exec.Stack.Push(NotEqual(exec));
                        break;
                    case OpCode.Less:
                        exec.Stack.Push(Less(exec, false));
                        break;
                    case OpCode.More:
                        exec.Stack.Push(Greater(exec, false));
                        break;
                    case OpCode.LessEq:
                        exec.Stack.Push(Less(exec, true));
                        break;
                    case OpCode.MoreEq:
                        exec.Stack.Push(Greater(exec, true));
                        break;
                    case OpCode.PushStackTopToOutput:
                        PushToOutput(exec, exec.Stack.Pop());
                        break;
                    case OpCode.PushStackTopToGrouping:
                        PushToGrouping(exec, exec.Stack.Pop());
                        break;
                    case OpCode.PushStackTopToOrdering:
                        PushToOrdering(exec, exec.Stack.Pop());
                        break;
                    case OpCode.PushColumn:
                        sourceContext = GetContextForLevel(exec, (int)instruction.Arg3);
                        exec.Stack.Push(sourceContext.MainSource.DataProvider.Read((int)instruction.Arg2));
                        break;
                    case OpCode.ExecuteSubQuery:
                        var sub = new ExecutionContext(Context)
                        {
                            Previous = exec
                        };
                        ExecuteQuery(sub, instruction.SubQuery);

                        // Get first column of the last row in the target result
                        if (sub.QueryOutput.Rows.Count > 0)
                        {
                            var result = sub.QueryOutput.Rows.Last()[0];
                            exec.Stack.Push(result);
                        }
                        else
                        {
                            exec.Stack.Push(new ColumnValue { Kind = ValueKind.Null });
                        }
                        break;
                    default:
                        throw new Exception("Unrecognized opcode");
                }
            }
        }
    }
}
