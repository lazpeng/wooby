using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby
{
    public enum ValueKind
    {
        Text,
        Number,
        Boolean,
        Null,
    }

    public class ColumnValue
    {
        public ValueKind Kind { get; set; }
        public string Text { get; set; }
        public double Number { get; set; }
        public bool Boolean { get; set; }

        public string PrettyPrint()
        {
            return Kind switch
            {
                ValueKind.Null => "",
                ValueKind.Number => $"{Number}",
                ValueKind.Text => Text,
                ValueKind.Boolean => Boolean ? "TRUE" : "FALSE",
                _ => ""
            };
        }
    }

    public class OutputColumnMeta
    {
        public string OutputName { get; set; }
        public ValueKind Kind { get; set; }
        public bool Visible { get; set; }
    }

    public class Output
    {
        public List<OutputColumnMeta> Definition { get; set; } = new List<OutputColumnMeta>();
        public List<List<ColumnValue>> Rows { get; set; } = new List<List<ColumnValue>>();
    }

    public abstract class DynamicVariable
    {
        public abstract ColumnValue WhenCalled(ExecutionContext context);
        public ColumnType ResultType { get; protected set; }
        public string Name { get; protected set; }
        public readonly long Id;

        public DynamicVariable(long Id)
        {
            this.Id = Id;
        }
    }

    class CurrentDate_Variable : DynamicVariable
    {
        public CurrentDate_Variable(long Id) : base(Id)
        {
            Name = "CURRENT_DATE";
            ResultType = ColumnType.String;
        }

        public override ColumnValue WhenCalled(ExecutionContext _)
        {
            var currentDate = DateTime.Now.ToString("u");
            return new ColumnValue() { Kind = ValueKind.Text, Text = currentDate };
        }
    }

    class DatabaseName_Variable : DynamicVariable
    {
        public DatabaseName_Variable(long Id) : base(Id)
        {
            Name = "DBNAME";
            ResultType = ColumnType.String;
        }

        public override ColumnValue WhenCalled(ExecutionContext _)
        {
            return new ColumnValue() { Kind = ValueKind.Text, Text = "wooby" };
        }
    }

    public interface ITableDataProvider
    {
        void Reset();
        bool SeekNext();
        ColumnValue GetColumn(int index);
        long RowId();
        bool Seek(long RowId);
        List<ColumnValue> WholeRow();
    }

    public class Dual_DataProvider : ITableDataProvider
    {
        public ColumnValue GetColumn(int index)
        {
            return null;
        }

        public void Reset()
        {
            // Stub
        }

        public long RowId()
        {
            return -1;
        }

        public bool Seek(long RowId)
        {
            return false;
        }

        public bool SeekNext()
        {
            return false;
        }

        public List<ColumnValue> WholeRow()
        {
            return null;
        }
    }

    public class LoveLive_DataProvider : ITableDataProvider
    {
        private class Group
        {
            public string Nome;
            public int Ano;
            public int NumIntegrantes;
        }

        private List<Group> grupos = new()
        {
            new Group() { Nome = "μ's", Ano = 2010, NumIntegrantes = 9 },
            new Group() { Nome = "Aqours", Ano = 2016, NumIntegrantes = 9 },
            new Group() { Nome = "Nijigasaki School Idol Club", Ano = 2017, NumIntegrantes = 10 },
            new Group() { Nome = "Liella", Ano = 2020, NumIntegrantes = 5 },
        };

        private int cursor = -1;

        public ColumnValue GetColumn(int index)
        {
            if (cursor < 0)
            {
                throw new InvalidOperationException();
            }

            var grupo = grupos[cursor];

            return index switch
            {
                0 => new ColumnValue() { Kind = ValueKind.Text, Text = grupo.Nome },
                1 => new ColumnValue() { Kind = ValueKind.Number, Number = grupo.Ano },
                2 => new ColumnValue() { Kind = ValueKind.Number, Number = grupo.NumIntegrantes },
                _ => throw new InvalidOperationException()
            };
        }

        public void Reset()
        {
            cursor = -1;
        }

        public long RowId()
        {
            return cursor;
        }

        public bool Seek(long RowId)
        {
            if (RowId < grupos.Count)
            {
                cursor = (int)RowId;
                return true;
            }
            else return false;
        }

        public bool SeekNext()
        {
            return ++cursor < grupos.Count;
        }

        public List<ColumnValue> WholeRow()
        {
            if (cursor < 0)
            {
                throw new InvalidOperationException();
            }

            var grupo = grupos[cursor];

            return new()
            {
                new ColumnValue() { Kind = ValueKind.Text, Text = grupo.Nome },
                new ColumnValue() { Kind = ValueKind.Number, Number = grupo.Ano },
                new ColumnValue() { Kind = ValueKind.Number, Number = grupo.NumIntegrantes }
            };
        }
    }

    public class TableData
    {
        public TableMeta Meta { get; set; }
        public ITableDataProvider DataProvider { get; set; }
    }

    public enum OpCode : int
    {
        SelectSourceTable = 0,
        PushCheckpointNext,
        CheckpointEnd,
        NewOutputRow,
        PushColumnToOutput,
        PushVariableToOutput,
        TrySeekElseSkip,
        SkipToNextAndPopIfNotTrue,
        SortByOutputAsc,
        SortByOutputDesc,
        AddOutputColumnDefinition,
        PushNumber,
        PushString,
        PushColumn,
        PushVariable,
        Sum,
        Sub,
        Div,
        Mul,
        Concat,
        Eq,
        NEq,
        LessEq,
        Less,
        MoreEq,
        More,
        AuxLessNumber,
        AuxLessEqNumber,
        AuxMoreNumber,
        AuxMoreEqNumber,
        AuxEqualNumber,
        PushStackTopToOutput,
        ResetAllProviders,
    }

    public class Instruction
    {
        public OpCode OpCode { get; set; }
        public long Arg1 { get; set; }
        public long Arg2 { get; set; }
        public long Arg3 { get; set; }
        public string Str1 { get; set; }
        public double Num1 { get; set; }
    }

    public class ExecutionContext
    {
        public Output QueryOutput { get; } = new Output();
        public int RowsAffected { get; set; } = 0;
        public Context Context { get; }
        public TableData MainSource { get; set; }
        public Stack<long> Checkpoints { get; set; } = new Stack<long>();
        public Stack<ColumnValue> Stack { get; set; } = new Stack<ColumnValue>();
        public bool LastComparisionResult { get; set; } = false;

        public ExecutionContext(Context ctx)
        {
            Context = ctx;
        }
    }

    public class Machine
    {
        private List<DynamicVariable> Variables { get; set; }
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

            Variables = new List<DynamicVariable>()
            {
                new CurrentDate_Variable(id++),
                new DatabaseName_Variable(id++),
            };

            foreach (var v in Variables)
            {
                Context.Variables.Add(new GlobalVariable() { Id = v.Id, Name = v.Name, Type = v.ResultType });
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

        private static int SkipUntilNextEnd(List<Instruction> instructions, int current)
        {
            do
            {
                if (instructions[current++].OpCode == OpCode.CheckpointEnd || current == instructions.Count - 1)
                {
                    break;
                }
            } while (true);

            return current - 1;
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

        private static ColumnValue More(ExecutionContext context, bool orEqual)
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

            throw new ArgumentException();
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

            throw new ArgumentException();
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

            throw new ArgumentException();
        }

        public ExecutionContext Execute(List<Instruction> instructions)
        {
            var exec = new ExecutionContext(Context);

            for (int i = 0; i < instructions.Count; ++i)
            {
                var instruction = instructions[i];

                switch (instruction.OpCode)
                {
                    case OpCode.SelectSourceTable:
                        if (exec.MainSource != null)
                        {
                            throw new InvalidOperationException("MainSource is already set");
                        }

                        exec.MainSource = Tables.Find(t => t.Meta.Id == instruction.Arg1);
                        if (exec.MainSource == null)
                        {
                            throw new Exception("SelectSourceTable: Could not find source table with given id");
                        }
                        break;
                    case OpCode.PushCheckpointNext:
                        exec.Checkpoints.Push(i);
                        break;
                    case OpCode.TrySeekElseSkip:
                        if (!exec.MainSource.DataProvider.SeekNext() && exec.QueryOutput.Rows.Count > 0)
                        {
                            i = SkipUntilNextEnd(instructions, i);
                            continue;
                        }
                        break;
                    case OpCode.NewOutputRow:
                        exec.QueryOutput.Rows.Add(new List<ColumnValue>());
                        break;
                    case OpCode.AddOutputColumnDefinition:
                        var definition = new OutputColumnMeta() { OutputName = instruction.Str1, Visible = true, Kind = ValueKind.Text };
                        exec.QueryOutput.Definition.Add(definition);
                        break;
                    case OpCode.CheckpointEnd:
                        i = (int)exec.Checkpoints.Peek();
                        break;
                    case OpCode.PushColumnToOutput:
                        PushToOutput(exec, exec.MainSource.DataProvider.GetColumn((int)instruction.Arg2));
                        break;
                    case OpCode.PushVariableToOutput:
                        PushToOutput(exec, Variables.Find(v => v.Id == instruction.Arg1).WhenCalled(exec));
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
                    case OpCode.SkipToNextAndPopIfNotTrue:
                        if (exec.Stack.TryPop(out ColumnValue value))
                        {
                            if (value.Kind == ValueKind.Boolean && !value.Boolean)
                            {
                                exec.QueryOutput.Rows.RemoveAt(exec.QueryOutput.Rows.Count - 1);
                                i = (int)exec.Checkpoints.Peek();
                            }
                        }
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
                        exec.Stack.Push(More(exec, false));
                        break;
                    case OpCode.LessEq:
                        exec.Stack.Push(Less(exec, true));
                        break;
                    case OpCode.MoreEq:
                        exec.Stack.Push(More(exec, true));
                        break;
                    case OpCode.PushStackTopToOutput:
                        PushToOutput(exec, exec.Stack.Pop());
                        break;
                    case OpCode.PushColumn:
                        exec.Stack.Push(exec.MainSource.DataProvider.GetColumn((int)instruction.Arg2));
                        break;
                    case OpCode.PushVariable:
                        exec.Stack.Push(Variables.Find(v => v.Id == instruction.Arg1).WhenCalled(exec));
                        break;
                    case OpCode.ResetAllProviders:
                        exec.MainSource.DataProvider.Reset();
                        break;
                    default:
                        throw new Exception("Unrecognized opcode");
                }
            }

            CheckOutputRows(exec);

            return exec;
        }
    }
}
