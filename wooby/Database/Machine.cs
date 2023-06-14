using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using wooby.Database.Persistence;
using wooby.Database.Defaults;
using wooby.Parsing;

namespace wooby.Database
{
    public class Machine
    {
        public Context Context { get; private set; }

        private static readonly List<Operator> BooleanOperators = new List<Operator>
        {
            Operator.Equal, Operator.NotEqual, Operator.LessEqual, Operator.MoreEqual, Operator.MoreThan,
            Operator.LessThan
        };

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
            if (context.QueryOutput.Rows.Count == 1 && context.QueryOutput.Rows[0].Values.All(v => v.Kind == ValueKind.Null))
            {
                context.QueryOutput.Rows.RemoveAt(0);
            }
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
                return x != null && y != null && x.Kind == y.Kind && Equal(x, y).Boolean;
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

                return new ColumnValue() {Number = lnum + rnum, Kind = ValueKind.Number};
            }
            else if (left.Kind == ValueKind.Text)
            {
                var lstr = left.Text;
                var rstr = right.Text;
                return new ColumnValue() {Text = lstr + rstr, Kind = ValueKind.Text};
            }

            throw new ArgumentException("Incompatible values for sum operation");
        }


        private static ColumnValue Equal(ColumnValue left, ColumnValue right)
        {
            AssertValuesNotBoolean(left, right);
            if (AnyValuesNull(left, right))
            {
                var equal = AnyValuesNull(left, left) && AnyValuesNull(right, right);
                return new ColumnValue() {Kind = ValueKind.Boolean, Boolean = equal};
            }

            if (left.Kind == ValueKind.Number)
            {
                var lnum = left.Number;
                var rnum = right.Number;

                return new ColumnValue() {Boolean = Math.Abs(lnum - rnum) < 0.001, Kind = ValueKind.Boolean};
            }
            else if (left.Kind == ValueKind.Text)
            {
                var lstr = left.Text;
                var rstr = right.Text;
                return new ColumnValue() {Boolean = lstr == rstr, Kind = ValueKind.Boolean};
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
                return new ColumnValue {Kind = ValueKind.Boolean, Boolean = false};
            }

            var result = new ColumnValue() {Boolean = false, Kind = ValueKind.Boolean};

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
                return new ColumnValue {Kind = ValueKind.Boolean, Boolean = false};
            }

            var result = new ColumnValue() {Boolean = false, Kind = ValueKind.Boolean};

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
                    result.Boolean = string.Compare(left.Text, right.Text, CultureInfo.InvariantCulture,
                        CompareOptions.None) > 0;
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

                return new ColumnValue() {Number = lnum / rnum, Kind = ValueKind.Number};
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

                return new ColumnValue() {Number = lnum % rnum, Kind = ValueKind.Number};
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

                return new ColumnValue() {Number = lnum * rnum, Kind = ValueKind.Number};
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

                return new ColumnValue() {Number = lnum - rnum, Kind = ValueKind.Number};
            }
            else if (left.Kind == ValueKind.Text)
            {
                throw new Exception("Invalid operation between strings");
            }

            throw new ArgumentException("Invalid arguments provided for subtraction");
        }

        private static void PrepareQueryOutput(ExecutionContext exec, Expression expr)
        {
            var id = expr.Identifier;
            if (string.IsNullOrEmpty(id))
            {
                id = expr.FullText;
            }

            exec.QueryOutput.Definition.Add(new OutputColumnMeta() {OutputName = id, Visible = true});
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
                    DistinctValue = group.Key,
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
            var newColumns = new Dictionary<int, ColumnValue>();
            var table = exec.Context.FindTable(insert.MainSource);
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

            SetupMainSource(exec, insert.MainSource);

            exec.MainSource.DataProvider.Insert(newColumns);
            exec.RowsAffected = 1;
        }

        private void ExecuteUpdate(ExecutionContext exec, UpdateStatement update)
        {
            SetupMainSource(exec, update.MainSource);

            var flags = new EvaluationFlags {Origin = ExpressionOrigin.Filter, Phase = QueryEvaluationPhase.Final};

            exec.QueryOutput.Rows.Add(new());
            while (exec.MainSource.DataProvider.SeekNext())
            {
                if (update.FilterConditions != null)
                {
                    var filter = EvaluateExpression(exec, update.FilterConditions, null, flags);
                    if (filter.Kind != ValueKind.Boolean)
                    {
                        throw new Exception("Expected boolean result for WHERE clause expression");
                    }

                    if (!filter.Boolean)
                    {
                        continue;
                    }
                }
                
                var dict = new Dictionary<int, ColumnValue>();

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
            SetupMainSource(exec, delete.MainSource);
            int affected = 0;

            while (exec.MainSource.DataProvider.SeekNext())
            {
                if (delete.FilterConditions != null)
                {
                    var filter = EvaluateExpression(exec, delete.FilterConditions, null,
                        new EvaluationFlags {Origin = ExpressionOrigin.Filter, Phase = QueryEvaluationPhase.Final});
                    if (filter.Kind != ValueKind.Boolean)
                    {
                        throw new Exception("Expected boolean result for WHERE clause expression");
                    }

                    if (!filter.Boolean)
                    {
                        continue;
                    }
                }
                affected += 1;
                exec.MainSource.DataProvider.Delete();
            }

            exec.RowsAffected = affected;
        }

        private ColumnValue ReadColumnReference(ExecutionContext exec, ColumnReference reference)
        {
            var sourceContext = GetContextForLevel(exec, reference.ParentLevel);
            if (sourceContext.RowNumber > sourceContext.TempRows.Count - 1 || !sourceContext
                    .TempRows[sourceContext.RowNumber].EvaluatedReferences
                    .TryGetValue(reference.Join(), out ColumnValue value))
            {
                var meta = exec.Context.FindColumn(reference);
                value = sourceContext.MainSource.DataProvider.Read(meta.Id);
            }

            return value;
        }

        private ColumnValue EvaluateFunctionCall(ExecutionContext exec, FunctionCall call, TempRow temp,
            EvaluationFlags flags)
        {
            var validAggregate = flags.Phase == QueryEvaluationPhase.Final &&
                                 (flags.Origin == ExpressionOrigin.Ordering ||
                                  flags.Origin == ExpressionOrigin.OutputColumn);
            if (call.CalledVariant.IsAggregate && !validAggregate)
            {
                throw new Exception("Illegal use of aggregate function here");
            }

            var arguments = new List<ColumnValue>();
            if (call.CalledVariant.IsAggregate)
            {
                // Instead, we pass an identifier for each expression
                arguments.AddRange(
                    call.Arguments.Select(a => new ColumnValue
                        {Kind = ValueKind.Text, Text = FormatTempRowReferenceAggFunc(call, a)}));
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
            if (sub.QueryOutput.Rows.Any())
            {
                var result = sub.QueryOutput.Rows.Last().Values[0];
                exec.Stack.Push(result);
            }
            else
            {
                exec.Stack.Push(new ColumnValue {Kind = ValueKind.Null});
            }
        }

        private void PushNodeColumnValueToStack(ExecutionContext exec, Expression.Node node, TempRow tempRow)
        {
            switch (node.Kind)
            {
                case Expression.NodeKind.Number:
                    exec.Stack.Push(new ColumnValue {Kind = ValueKind.Number, Number = node.NumberValue});
                    break;
                case Expression.NodeKind.String:
                    exec.Stack.Push(new ColumnValue {Kind = ValueKind.Text, Text = node.StringValue});
                    break;
                case Expression.NodeKind.Reference:
                    if (tempRow == null ||
                        !tempRow.EvaluatedReferences.TryGetValue(node.ReferenceValue.Join(), out ColumnValue value))
                    {
                        value = ReadColumnReference(exec, node.ReferenceValue);
                    }

                    exec.Stack.Push(value);
                    break;
            }
        }

        private int EvaluateSubExpression(int offset, ExecutionContext exec, Expression expr, TempRow tempRow,
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
                            !tempRow.EvaluatedReferences.TryGetValue(call.FullText, out ColumnValue value))
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
                    }
                }
                else
                {
                    if (node.Kind == Expression.NodeKind.Function)
                    {
                        if (tempRow != null)
                        {
                            var call = node.FunctionCall;
                            if (!tempRow.EvaluatedReferences.TryGetValue(call.FullText, out ColumnValue value))
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

        private ColumnValue EvaluateExpression(ExecutionContext exec, Expression expr, TempRow temp,
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

        private void ExecuteQuery(ExecutionContext exec, SelectStatement query)
        {
            SetupMainSource(exec, query.MainSource);
            exec.ResetRowNumber();

            foreach (var output in query.OutputColumns)
            {
                PrepareQueryOutput(exec, output);
            }

            // Gather required info for all rows that match the filter
            while (exec.MainSource.DataProvider.SeekNext())
            {
                if (query.FilterConditions != null)
                {
                    var filter = EvaluateExpression(exec, query.FilterConditions, null,
                        new EvaluationFlags {Origin = ExpressionOrigin.Filter, Phase = QueryEvaluationPhase.Final});
                    if (filter.Kind != ValueKind.Boolean)
                    {
                        throw new Exception("Expected boolean result for WHERE clause expression");
                    }

                    if (!filter.Boolean)
                    {
                        continue;
                    }
                }

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

        private void SetupMainSource(ExecutionContext exec, long tableId)
        {
            var sourceData = Context.FindTable(tableId);
            exec.MainSource = new ExecutionDataSource()
            {
                Meta = sourceData,
                DataProvider = new TableCursor(sourceData.DataProvider, sourceData.Columns.Count)
            };
        }

        private void SetupMainSource(ExecutionContext exec, ColumnReference source)
        {
            SetupMainSource(exec, Context.FindTable(source).Id);
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