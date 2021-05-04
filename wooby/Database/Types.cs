using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby.Database
{
    public enum ValueKind
    {
        Text,
        Number,
        Boolean,
        Date,
        Null,
    }

    public class ColumnValue
    {
        public ValueKind Kind { get; set; }
        public string Text { get; set; }
        public double Number { get; set; }
        public bool Boolean { get; set; }
        public DateTime Date { get; set; }

        public string PrettyPrint()
        {
            return Kind switch
            {
                ValueKind.Null => "",
                ValueKind.Number => $"{Number}",
                ValueKind.Text => Text,
                ValueKind.Boolean => Boolean ? "TRUE" : "FALSE",
                ValueKind.Date => Date.ToString("u"),
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

    public abstract class Function
    {
        public abstract ColumnValue WhenCalled(ExecutionContext context, List<ColumnValue> arguments);
        public ColumnType ResultType { get; protected set; }
        public string Name { get; protected set; }
        public List<ColumnType> Parameters { get; set; }
        public readonly long Id;

        public Function(long Id)
        {
            this.Id = Id;
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

    public enum OpCode : int
    {
        PushColumnToOutput,
        CallFunction,
        PushNumber,
        PushString,
        PushColumn,
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

    public class TableData
    {
        public TableMeta Meta { get; set; }
        public ITableDataProvider DataProvider { get; set; }
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
}
