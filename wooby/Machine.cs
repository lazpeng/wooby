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
        Null,
    }

    public class ColumnValue
    {
        public ValueKind Kind { get; set; }
        public string Text { get; set; }
        public double Number { get; set; }
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

    class CurrentTime_Variable : DynamicVariable
    {
        public CurrentTime_Variable(long Id) : base(Id)
        {
            Name = "CURRENT_TIME";
            ResultType = ColumnType.String;
        }

        public override ColumnValue WhenCalled(ExecutionContext _)
        {
            var time = DateTimeOffset.Now.ToString("hh:mm:sszzz");
            return new ColumnValue() { Kind = ValueKind.Text, Text = time };
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
        SkipToNextIf,
        SortByOutputAsc,
        SortByOutputDesc,
        AddOutputColumnDefinition,
        PushNumber,
        PushString,
        Sum,
        Sub,
        Div,
        Mul,
        Concat,
        AuxLessNumber,
        AuxLessEqNumber,
        AuxMoreNumber,
        AuxMoreEqNumber,
        AuxEqualNumber,
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
        public Stack<double> NumberStack { get; set; } = new Stack<double>();
        public Stack<string> StringStack { get; set; } = new Stack<string>();
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
                new CurrentTime_Variable(id++),
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
                }
            };

            foreach (var t in Tables)
            {
                Context.AddTable(t.Meta, Context.Schemas[0]);
            }
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
                if (instructions[current].OpCode == OpCode.CheckpointEnd || current == instructions.Count - 1)
                {
                    break;
                }

                ++current;
            } while (true);

            return current;
        }

        private static void PushToOutput(ExecutionContext context, ColumnValue value)
        {
            if (context.QueryOutput.Rows.Count == 0)
            {
                throw new InvalidOperationException("Query output has no rows to push to");
            }

            context.QueryOutput.Rows.Last().Add(value);
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
                        exec.Checkpoints.Push(i + 1);
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
                        break;
                    case OpCode.PushColumnToOutput:
                        break;
                    case OpCode.PushVariableToOutput:
                        PushToOutput(exec, Variables.Find(v => v.Id == instruction.Arg1).WhenCalled(exec));
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
