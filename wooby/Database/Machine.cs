using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

using wooby.Parsing;

namespace wooby.Database
{
    public class Machine
    {
        private List<Function> Functions { get; set; }
        private List<TableData> Tables { get; set; }
        public Context Context { get; private set; } = null;

        public Context Initialize()
        {
            Context = new Context();

            InitializeVariables();
            InitializeTables();

            return Context;
        }

        private void InitializeVariables()
        {
            long id = 0;

            Functions = new List<Function>()
            {
                new CurrentDate_Function(id++),
                new DatabaseName_Function(id++),
                new RowNum_Function(id++),
                new RowId_Function(id++),
                new Trunc_Function(id++),
            };

            foreach (var v in Functions)
            {
                Context.Functions.Add(new wooby.Function() { Id = v.Id, Name = v.Name, Type = v.ResultType, Parameters = v.Parameters });
            }
        }

        private void InitializeTables()
        {
            Tables = new List<TableData>();

            var dualMeta = new TableMeta()
            {
                Name = "dual",
                Columns = new List<ColumnMeta>(),
                IsReal = false,
                IsTemporary = false
            };
            Context.AddTable(dualMeta);
            Tables.Add(new TableData { Meta = dualMeta, DataProvider = new Dual_DataProvider() });

            var loveliveMeta = new TableMeta() { Name = "lovelive" };
            Context.AddColumn(new ColumnMeta() { Name = "id", Type = ColumnType.Number }, loveliveMeta);
            Context.AddColumn(new ColumnMeta() { Name = "parent_id", Type = ColumnType.Number }, loveliveMeta);
            Context.AddColumn(new ColumnMeta() { Name = "nome", Type = ColumnType.String }, loveliveMeta);
            Context.AddColumn(new ColumnMeta() { Name = "ano", Type = ColumnType.Number }, loveliveMeta);
            Context.AddColumn(new ColumnMeta() { Name = "integrantes", Type = ColumnType.Number }, loveliveMeta);
            Context.AddTable(loveliveMeta);
            Tables.Add(new TableData() { Meta = loveliveMeta, DataProvider = new LoveLive_DataProvider(loveliveMeta) });
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
            if (expr.IsWildcard())
            {
                if (expr.IsOnlyReference())
                {
                    var reference = expr.Nodes[0].ReferenceValue;

                    var table = Context.FindTable(reference);

                    foreach (var col in table.Columns)
                    {
                        exec.QueryOutput.Definition.Add(new OutputColumnMeta() { OutputName = col.Name, Visible = true });
                    }
                }
                else
                {
                    // Push columns for all tables in select command

                    foreach (var col in Context.FindTable(query.MainSource).Columns)
                    {
                        exec.QueryOutput.Definition.Add(new OutputColumnMeta() { OutputName = col.Name, Visible = true });
                    }
                }
            }
            else
            {
                var id = expr.Identifier;
                if (string.IsNullOrEmpty(id))
                {
                    id = expr.FullText;
                }

                exec.QueryOutput.Definition.Add(new OutputColumnMeta() { OutputName = id, Visible = true });
            }
        }

        private static List<RowOrderingIntermediate> BuildFromRows(ExecutionContext exec, SelectStatement query, List<int> indexes, int colIndex)
        {
            var result = new List<RowOrderingIntermediate>();
            var ascending = query.OutputOrder[colIndex].Kind == OrderingKind.Ascending;

            IEnumerable<RowOrderData> input;
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
                var colmeta = new ColumnMeta
                {
                    Name = col.Name,
                    Type = col.Type,
                    Parent = meta,
                };

                Context.AddColumn(colmeta, meta);
            }

            exec.Context.AddTable(meta);
            Tables.Add(new TableData
            {
                Meta = meta,
                DataProvider = new InMemory_DataProvider(meta)
            });
        }

        private void ExecuteInsert(ExecutionContext exec, InsertStatement insert)
        {
            var newColumns = new Dictionary<int, ColumnValue>();

            if (insert.Columns.Count != insert.Values.Count)
            {
                throw new Exception("Error: Different length for Columns list as Values list");
            }

            var table = exec.Context.FindTable(insert.MainSource);
            if (table == null)
            {
                throw new Exception("Could not find table");
            }

            for (int i = 0; i < insert.Columns.Count; ++i)
            {
                var idx = table.Columns.FindIndex(c => c.Name == insert.Columns[i]);
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
            throw new NotImplementedException();
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
                Execute(filter, exec);

                var top = exec.PopStack(true);
                if (filter.Count == 0 || (top != null && top.Kind == ValueKind.Boolean && top.Boolean))
                {
                    affected += 1;
                    exec.MainSource.DataProvider.Delete();
                }
            }

            exec.RowsAffected = affected;
        }

        private void ExecuteQuery(ExecutionContext exec, SelectStatement query)
        {
            // Compile all expressions
            var outputExpressions = new List<Instruction>();

            foreach (var output in query.OutputColumns)
            {
                Compiler.CompileExpression(query, output, Context, outputExpressions, PushResultKind.ToOutput);

                // Prepare the output columns

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

            // Select main source

            SetupMainSource(exec, query.MainSource);

            // First, filter all columns in the source if a filter was specified
            var filteredRows = new List<long>();

            if (query.FilterConditions != null)
            {
                while (exec.MainSource.DataProvider.SeekNext())
                {
                    // Add output rows so we can use ROWNUM in the WHERE clause
                    exec.QueryOutput.Rows.Add(new List<ColumnValue>());
                    Execute(filter, exec);

                    if (exec.Stack.TryPop(out ColumnValue value))
                    {
                        if (value.Kind == ValueKind.Boolean && value.Boolean)
                        {
                            filteredRows.Add(exec.MainSource.DataProvider.CurrentRowId());
                        }
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
                        break;
                    else
                        exec.MainSource.DataProvider.Seek(filteredRows[filterIndex++]);
                }
                else if (!exec.MainSource.DataProvider.SeekNext())
                    break;

                var rowIndex = exec.QueryOutput.Rows.Count;
                // First get the actual outputs from the query
                exec.QueryOutput.Rows.Add(new List<ColumnValue>());
                Execute(outputExpressions, exec);
                // Fetch the required results for the ordering
                exec.OrderingResults.Add(new RowOrderData() { RowIndex = rowIndex });
                Execute(ordering, exec);
            }

            OrderOutputRows(exec, query);
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
