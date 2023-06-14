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
        public abstract void WhenCalled(ExecutionContext context, ResultCallback OnResult);
        public ColumnType ResultType { get; protected set; }
        public string Name { get; protected set; }
        public readonly long Id;

        public DynamicVariable(long Id)
        {
            this.Id = Id;
        }

        public delegate void ResultCallback(ColumnValue Value);
    }

    class CurrentDate_Variable : DynamicVariable
    {
        public CurrentDate_Variable(long Id) : base(Id)
        {
            Name = "CURRENT_DATE";
            ResultType = ColumnType.String;
        }

        public override void WhenCalled(ExecutionContext _, ResultCallback OnResult)
        {
            var currentDate = DateTimeOffset.Now.ToString();
            OnResult(new ColumnValue() { Kind = ValueKind.Text, Text = currentDate });
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

    public enum OpCode
    {
        SelectSourceTable,
        PushCheckpointNext,
        CheckpointEnd,
        NewOutputRow,
        PushColumnToOutput,
        PushVariableToOutput,
        LoadColumnIntoAux,
        LoadVariableIntoAux,
        SkipToEndIfCantSeek,
        SkipToNextIf,
        SortByOutputAsc,
        SortByOutputDesc,
        // Comparisions
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
        public int RowsAffected { get; set; }
        public Context Context { get; }
        public TableData MainSource { get; set; }
        public Stack<long> Checkpoints { get; set; }
        public ColumnValue AuxiliaryValue { get; set; }
        public bool LastComparisionResult { get; set; }
    }

    public class Machine
    {
        private List<DynamicVariable> Variables { get; set; }
        private List<TableData> Tables { get; set; }
        public Context Context { get; private set; } = null;

        public Context Initialize()
        {
            var ctx = new Context();

            InitializeVariables();
            InitializeTables();

            Context = ctx;
            return ctx;
        }

        private void InitializeVariables()
        {
            long id = 0;

            Variables = new List<DynamicVariable>()
            {
                new CurrentDate_Variable(id++)
            };

            foreach (var v in Variables)
            {
                Context.Variables.Add(new GlobalVariable() { Id = v.Id, Name = v.Name, Type = v.ResultType });
            }
        }

        private void InitializeTables()
        {
            long id = 0;

            Tables = new List<TableData>()
            {
                new TableData()
                {
                    Meta = new TableMeta()
                    {
                        Name = "dual",
                        Columns = new List<ColumnMeta>(),
                        Id = id++,
                        IsReal = false,
                        IsTemporary = false
                    },
                    DataProvider = new Dual_DataProvider()
                }
            };

            foreach (var t in Tables)
            {
                Context.Schemas[0].Tables.Add(t.Meta);
            }
        }

        public ExecutionContext Execute(List<Instruction> instructions)
        {
            var exec = new ExecutionContext();

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
                    default:
                        throw new Exception("Unrecognized opcode");
                }
            }

            return exec;
        }
    }
}
