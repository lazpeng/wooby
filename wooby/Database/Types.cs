using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using wooby.Database.Persistence;
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

    public class TempRow
    {
        public Dictionary<string, ColumnValue> EvaluatedReferences = new ();
        public long RowId;
        public int RowIndex;
    }

    public enum QueryEvaluationPhase
    {
        Caching,
        Final
    }

    public enum ExpressionOrigin
    {
        OutputColumn,
        Ordering,
        Filter,
        Grouping
    }

    public class EvaluationFlags
    {
        public QueryEvaluationPhase Phase { get; set; }
        public ExpressionOrigin Origin { get; set; }
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
    public class RowMetaData
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
        public List<ColumnType> Parameters { get; protected set; }
        public long Id { get; protected set; }
        public bool IsAggregate { get; protected set; } = false;

        public Function(long Id)
        {
            this.Id = Id;
        }
    }

    public class TableCursor
    {
        private readonly ITableDataProvider Source;
        private readonly int NumCols;
        private long RowId = long.MinValue;
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

        public bool SeekFirst()
        {
            return Seek(long.MinValue);
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

        public void Delete()
        {
            RowId = Source.Delete(RowId);
        }

        public void Update(Dictionary<int, ColumnValue> values)
        {
            Source.Update(RowId, values);
        }

        public long Insert(Dictionary<int, ColumnValue> values)
        {
            var id = Source.Insert(values);
            if (id == long.MinValue || !Seek(id))
            {
                return long.MinValue;
            }

            return id;
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
        Rem,
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
        PushStackTopToGrouping,
        ExecuteSubQuery,
    }

    public enum PushResultKind
    {
        None,
        ToOutput,
        ToOrdering,
        ToGrouping,
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
        public Stack<ColumnValue> Stack { get; set; } = new Stack<ColumnValue>();
        public List<RowMetaData> OrderingResults { get; set; } = new List<RowMetaData>();
        public List<RowMetaData> GroupingResults { get; set; } = new List<RowMetaData>();
        public List<TempRow> TempRows { get; set; } = new List<TempRow>();
        public ExecutionContext Previous { get; set; } = null;
        public int RowNumber { get; private set; }

        public ExecutionContext(Context ctx)
        {
            Context = ctx;
        }

        public void ResetRowNumber()
        {
            RowNumber = 0;
        }

        public void IncrementRowNumber()
        {
            RowNumber += 1;
        }

        public TempRow CreateTempRow()
        {
            return new TempRow { RowId = MainSource.DataProvider.CurrentRowId(), RowIndex = RowNumber };
        }

        public ColumnValue PopStack(bool returnNull = false)
        {
            if (Stack.Count == 0)
            {
                if (returnNull)
                {
                    return null;
                } else
                {
                    throw new Exception("Stack is empty");
                }
            }

            return Stack.Pop();
        }
    }
}
