﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
                new SayHello_Function(id++),
            };

            foreach (var v in Functions)
            {
                Context.Functions.Add(new wooby.Function() { Id = v.Id, Name = v.Name, Type = v.ResultType, Parameters = v.Parameters });
            }
        }

        private void InitializeTables()
        {
            Tables = new List<TableData>()
            {
                new TableData()
                {
                    Meta = new TableMeta()
                    {
                        Name = "dual",
                        Columns = new List<ColumnMeta>(),
                        IsReal = false,
                        IsTemporary = false
                    },
                    DataProvider = new Dual_DataProvider()
                },
                new TableData()
                {
                    Meta = new TableMeta()
                    {
                        Name = "lovelive",
                    },
                    DataProvider = new LoveLive_DataProvider()
                }
            };

            foreach (var t in Tables)
            {
                Context.AddTable(t.Meta);
            }

            Context.AddColumn(new ColumnMeta() { Name = "nome", Type = ColumnType.String }, Tables[1].Meta);
            Context.AddColumn(new ColumnMeta() { Name = "ano", Type = ColumnType.Number }, Tables[1].Meta);
            Context.AddColumn(new ColumnMeta() { Name = "integrantes", Type = ColumnType.Number }, Tables[1].Meta);
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

        private static ColumnValue Sum(ExecutionContext context)
        {
            var right = context.Stack.Pop();
            var left = context.Stack.Pop();

            AssertValuesNotBoolean(left, right);

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

            throw new ArgumentException();
        }


        private static ColumnValue Equal(ColumnValue left, ColumnValue right)
        {
            AssertValuesNotBoolean(left, right);

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

            throw new ArgumentException();
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

        private static ColumnValue Greater(ExecutionContext context, bool orEqual)
        {
            var right = context.Stack.Pop();
            var left = context.Stack.Pop();

            AssertValuesNotBoolean(left, right);

            var result = new ColumnValue() { Boolean = false, Kind = ValueKind.Boolean };

            if (left.Kind == ValueKind.Number)
            {
                var lnum = left.Number;
                var rnum = right.Number;

                result.Boolean = lnum > rnum;
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

        private static ColumnValue Divide(ExecutionContext context)
        {
            var right = context.Stack.Pop();
            var left = context.Stack.Pop();

            AssertValuesNotBoolean(left, right);

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

        private static ColumnValue Multiply(ExecutionContext context)
        {
            var right = context.Stack.Pop();
            var left = context.Stack.Pop();

            AssertValuesNotBoolean(left, right);

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

        private void ExecuteQuery(ExecutionContext exec, SelectStatement query)
        {
            // Compile all expressions
            var outputExpressions = new List<Instruction>();

            exec.QueryOutput.Definition.Add(new OutputColumnMeta() { Kind = ValueKind.Number, OutputName = "ROWID", Visible = false });

            foreach (var output in query.OutputColumns)
            {
                Compiler.CompileExpression(query, output, Context, outputExpressions, true);

                // Prepare the output columns

                PrepareQueryOutput(exec, query, output);
            }

            var filter = new List<Instruction>();
            if (query.FilterConditions != null)
            {
                Compiler.CompileExpression(query, query.FilterConditions, Context, filter, false);
            }

            // Select source

            var sourceId = Context.FindTable(query.MainSource).Id;
            exec.MainSource = Tables.Find(t => t.Meta.Id == sourceId);
            exec.MainSource.DataProvider.Reset();

            // First, filter all columns in the source if a filter was specified

            if (query.FilterConditions != null)
            {
                var filteredRows = new List<long>();

                while (exec.MainSource.DataProvider.SeekNext())
                {
                    // Add output rows so we can use ROWNUM in the WHERE clause
                    exec.QueryOutput.Rows.Add(new List<ColumnValue>());
                    Execute(filter, exec);

                    if (exec.Stack.TryPop(out ColumnValue value))
                    {
                        if (value.Kind == ValueKind.Boolean && value.Boolean)
                        {
                            filteredRows.Add(exec.MainSource.DataProvider.RowId());
                        }
                    }
                };

                exec.QueryOutput.Rows.Clear();

                // Revisit all filtered rows and select the results

                foreach (var rowid in filteredRows)
                {
                    exec.MainSource.DataProvider.Seek(rowid);

                    exec.QueryOutput.Rows.Add(new List<ColumnValue>());
                    exec.QueryOutput.Rows.Last().Add(new ColumnValue() { Kind = ValueKind.Number, Number = rowid });
                    Execute(outputExpressions, exec);
                }
            }
            else
            {
                while (exec.MainSource.DataProvider.SeekNext())
                {
                    var rowid = exec.MainSource.DataProvider.RowId();

                    exec.QueryOutput.Rows.Add(new List<ColumnValue>());
                    exec.QueryOutput.Rows.Last().Add(new ColumnValue() { Kind = ValueKind.Number, Number = rowid });
                    Execute(outputExpressions, exec);
                }
            }

            CheckOutputRows(exec);
        }

        public ExecutionContext Execute(Statement command)
        {
            var exec = new ExecutionContext(Context);

            if (command is SelectStatement query)
            {
                ExecuteQuery(exec, query);
            }

            return exec;
        }

        private List<ColumnValue> PopFunctionArguments(ExecutionContext exec, int numArgs)
        {
            var result = new List<ColumnValue>(numArgs);

            for (int i = 0; i < numArgs; ++i)
            {
                result.Add(exec.Stack.Pop());
            }

            result.Reverse();
            return result;
        }

        public void Execute(List<Instruction> instructions, ExecutionContext exec)
        {
            for (int i = 0; i < instructions.Count; ++i)
            {
                var instruction = instructions[i];

                switch (instruction.OpCode)
                {
                    case OpCode.PushColumnToOutput:
                        PushToOutput(exec, exec.MainSource.DataProvider.GetColumn((int)instruction.Arg2));
                        break;
                    case OpCode.CallFunction:
                        var numArgs = (int) instruction.Arg2;
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
                    case OpCode.PushColumn:
                        exec.Stack.Push(exec.MainSource.DataProvider.GetColumn((int)instruction.Arg2));
                        break;
                    default:
                        throw new Exception("Unrecognized opcode");
                }
            }
        }
    }
}