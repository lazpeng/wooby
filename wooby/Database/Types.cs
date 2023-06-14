using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using wooby.Parsing;

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

        public static ColumnValue Null()
        {
            return new ColumnValue { Kind = ValueKind.Null };
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

    // Data for knowing how to order output rows
    public class RowOrderData
    {
        public List<ColumnValue> Values { get; set; } = new List<ColumnValue>();
        public int RowIndex { get; set; }
    }

    public class RowOrderingIntermediate
    {
        public ColumnValue DistinctValue { get; set; }
        public List<int> MatchingRows { get; set; }
        public List<RowOrderingIntermediate> SubOrdering { get; set; }

        public void Collect(ExecutionContext exec, List<List<ColumnValue>> target)
        {
            if (MatchingRows != null && MatchingRows.Count > 0)
            {
                foreach (var idx in MatchingRows)
                {
                    target.Add(exec.QueryOutput.Rows[idx]);
                }
            } else
            {
                foreach (var sub in SubOrdering)
                {
                    sub.Collect(exec, target);
                }
            }
        }
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
        IEnumerable<ColumnValue> Seek(long RowId);
        IEnumerable<ColumnValue> SeekNext(ref long RowId);
    }

    public class TableCursor
    {
        private readonly ITableDataProvider Source;
        private readonly int NumCols;
        private long RowId = -1;
        private IEnumerable<ColumnValue> CurrentValues;

        public TableCursor(ITableDataProvider Source, int NumCols)
        {
            this.Source = Source;
            this.NumCols = NumCols;
        }

        public bool Seek(long Id)
        {
            var result = Source.Seek(Id);
            if (result != null)
            {
                RowId = Id;
                CurrentValues = result;
            }
            return result != null;
        }

        public bool SeekNext()
        {
            long id = RowId;
            var result = Source.SeekNext(ref id);
            if (result != null)
            {
                RowId = id;
                CurrentValues = result;
            }
            return result != null;
        }

        public long CurrentRowId()
        {
            return RowId;
        }

        public ColumnValue Read(int Index)
        {
            if (Index >= NumCols)
            {
                throw new IndexOutOfRangeException("Index for column is out of range");
            }

            return CurrentValues.ElementAt(Index);
        }
    }

    public enum OpCode : int
    {
        PushColumnToOutput = 0,
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
        PushStackTopToOrdering,
        ExecuteSubQuery,
    }

    public enum PushResultKind
    {
        None,
        ToOutput,
        ToOrdering
    }

    public class Instruction
    {
        public OpCode OpCode { get; set; }
        public long Arg1 { get; set; }
        public long Arg2 { get; set; }
        public long Arg3 { get; set; }
        public string Str1 { get; set; }
        public double Num1 { get; set; }
        // LOL
        public SelectStatement SubQuery { get; set; }
    }

    public class TableData
    {
        public TableMeta Meta { get; set; }
        public ITableDataProvider DataProvider { get; set; }
    }

    public class ExecutionDataSource
    {
        public TableMeta Meta { get; set; }
        public TableCursor DataProvider { get; set; }
    }

    public class ExecutionContext
    {
        public Output QueryOutput { get; } = new Output();
        public int RowsAffected { get; set; } = 0;
        public Context Context { get; }
        public ExecutionDataSource MainSource { get; set; }
        public Stack<long> Checkpoints { get; set; } = new Stack<long>();
        public Stack<ColumnValue> Stack { get; set; } = new Stack<ColumnValue>();
        public List<RowOrderData> OrderingResults { get; set; } = new List<RowOrderData>();
        public ExecutionContext Previous { get; set; } = null;

        public ExecutionContext(Context ctx)
        {
            Context = ctx;
        }
    }
}
